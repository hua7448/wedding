#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
婚礼抽奖 · 后端服务（零依赖，仅需 Python 3.8+ 标准库）
=====================================================

功能：
  - 托管整个前端（本目录下的 html/css/js/img/audio 静态文件）
  - 宾客登记：姓名 + 手机号 + 设备指纹，集中发号（001 起，全服务器唯一）
  - 抽奖：服务端原子抽取，绝不重复，中奖立即落库
  - 大屏/控制台实时同步：SSE（Server-Sent Events）广播
  - 数据持久化：SQLite（data.db），每次变更同时导出 data_backup.json 双保险
  - 管理接口密码鉴权（默认 hn2026，可在 config.html 修改）

启动：
  python3 server.py              # 默认 8000 端口
  python3 server.py --port 8080

部署（natapp 等内网穿透）：将隧道指向本服务端口即可，宾客手机扫码直达。

接口一览（管理接口需请求头 X-Admin-Pass 或 ?pass= 携带管理密码）：
  GET  /api/ping                    心跳，前端据此判断已进入服务器模式
  GET  /api/config                  读取婚礼信息与奖项（公开，页面渲染用）
  POST /api/config                  保存配置（管理）
  POST /api/auth                    校验管理密码 {password}
  POST /api/lookup                  按指纹查已领券 {fp}
  POST /api/register                登记领券 {name, phone, wish, fp}
  GET  /api/state                   控制台快照（管理）：统计 + 中奖名单 + 各奖项进度
  POST /api/draw                    定格抽奖（管理）{prizeId, count, round} → 原子写入并广播
  POST /api/winner-status           兑奖/弃奖（管理）{id, status}
  POST /api/command                 大屏指令（管理）{t: roll|idle, ...} → 原样广播
  POST /api/reset                   清空宾客与中奖数据（管理）
  GET  /api/export/winners.csv      导出中奖记录（管理，Excel 可读）
  GET  /api/export/guests.csv       导出到场名单（管理）
  GET  /api/events                  SSE 事件流（大屏订阅，无需密码，只读指令）
"""
import argparse
import csv
import io
import json
import os
import queue
import secrets
import sqlite3
import threading
import time
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, 'data.db')
BACKUP_PATH = os.path.join(BASE, 'data_backup.json')

DB_LOCK = threading.Lock()
SUBS_LOCK = threading.Lock()
SUBS = []  # SSE 订阅者队列列表

CODE_CHARS = '23456789ABCDEFGHJKMNPQRSTUVWXYZ'  # 去掉易混淆的 0/O/1/I/L

DEFAULT_CONFIG = {
    'groom': '黄继安', 'bride': '聂玮婷',
    'groomEn': 'Huang Ji’an', 'brideEn': 'Nie Weiting',
    'date': '2026.10.25', 'venue': '',
    'title': '婚礼幸运抽奖',
    'adminPass': 'hn2026',
    'prizes': [
        {'id': 'p_san', 'name': '三等奖', 'count': 4, 'perRound': 2, 'desc': ''},
        {'id': 'p_er',  'name': '二等奖', 'count': 3, 'perRound': 2, 'desc': ''},
        {'id': 'p_yi',  'name': '一等奖', 'count': 2, 'perRound': 1, 'desc': ''},
        {'id': 'p_te',  'name': '特等奖', 'count': 1, 'perRound': 1, 'desc': ''},
    ],
}

# ------------------------------------------------------------ 数据库
CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
CONN.row_factory = sqlite3.Row
CONN.execute('PRAGMA journal_mode=WAL')   # 崩溃安全 + 读写并发
CONN.execute('PRAGMA synchronous=NORMAL')


def init_db():
    with DB_LOCK:
        CONN.executescript('''
        CREATE TABLE IF NOT EXISTS guests(
          id    INTEGER PRIMARY KEY AUTOINCREMENT,
          num   TEXT UNIQUE NOT NULL,
          name  TEXT NOT NULL,
          phone TEXT UNIQUE NOT NULL,
          wish  TEXT DEFAULT '',
          fp    TEXT,
          code  TEXT NOT NULL,
          ts    INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS winners(
          id         TEXT PRIMARY KEY,
          prize_id   TEXT, prize_name TEXT, prize_desc TEXT,
          round_label TEXT,
          num TEXT, name TEXT, phone TEXT, code TEXT,
          ts INTEGER, status TEXT DEFAULT 'win'
        );
        CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
        ''')
        CONN.commit()
        if not get_meta('config'):
            set_meta('config', json.dumps(DEFAULT_CONFIG, ensure_ascii=False))
        if not get_meta('next_num'):
            set_meta('next_num', '1')


def get_meta(k):
    r = CONN.execute('SELECT v FROM meta WHERE k=?', (k,)).fetchone()
    return r['v'] if r else None


def set_meta(k, v):
    CONN.execute('INSERT INTO meta(k,v) VALUES(?,?) '
                 'ON CONFLICT(k) DO UPDATE SET v=excluded.v', (k, v))
    CONN.commit()


def backup():
    """每次变更后导出全量 JSON 备份（双保险，可直接人工查看/恢复）"""
    try:
        data = {
            'exported_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'guests': [dict(r) for r in CONN.execute('SELECT * FROM guests ORDER BY id')],
            'winners': [dict(r) for r in CONN.execute('SELECT * FROM winners ORDER BY ts')],
            'next_num': get_meta('next_num'),
            'config': json.loads(get_meta('config') or '{}'),
        }
        tmp = BACKUP_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, BACKUP_PATH)
    except Exception as e:
        print('[backup] 备份失败（不影响主流程）:', e)


# ------------------------------------------------------------ SSE 广播
def broadcast(msg):
    dead = []
    with SUBS_LOCK:
        for q in SUBS:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            SUBS.remove(q)


# ------------------------------------------------------------ 业务逻辑
def rand_code(n=4):
    return ''.join(secrets.choice(CODE_CHARS) for _ in range(n))


def pad3(n):
    return ('00' + str(n))[-3:]


def row_to_guest(r):
    return {'num': r['num'], 'name': r['name'], 'phone': r['phone'],
            'wish': r['wish'] or '', 'code': r['code'], 'ts': r['ts']}


def pool_guests(cur=None):
    """号码池 = 已登记 - 有效中奖（弃奖自动回池）"""
    q = '''SELECT g.* FROM guests g WHERE g.num NOT IN
           (SELECT num FROM winners WHERE status != 'void') ORDER BY g.id'''
    rows = (cur or CONN).execute(q).fetchall()
    return [row_to_guest(r) for r in rows]


def get_config():
    cfg = json.loads(get_meta('config') or '{}')
    merged = dict(DEFAULT_CONFIG)
    merged.update(cfg)
    return merged


# ------------------------------------------------------------ HTTP 处理
class Handler(SimpleHTTPRequestHandler):
    server_version = 'WeddingLottery/2.0'

    def __init__(self, *a, **kw):
        super().__init__(*a, directory=BASE, **kw)

    # 静态文件禁缓存，避免大屏/控制台拿到旧页面
    def end_headers(self):
        if not self.path.startswith('/api/events'):
            self.send_header('Cache-Control', 'no-store')
        super().end_headers()

    def log_message(self, fmt, *args):
        if '/api/' in self.path:
            print('[%s] %s' % (time.strftime('%H:%M:%S'), fmt % args))

    # ---------- 工具 ----------
    def _json_body(self, limit=65536):
        ln = int(self.headers.get('Content-Length') or 0)
        if ln <= 0 or ln > limit:
            return None
        try:
            return json.loads(self.rfile.read(ln).decode('utf-8'))
        except Exception:
            return None

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _query(self):
        return parse_qs(urlparse(self.path).query)

    def _authed(self):
        cfg = get_config()
        pw = self.headers.get('X-Admin-Pass') or (self._query().get('pass', [''])[0])
        return bool(pw) and secrets.compare_digest(str(pw), str(cfg.get('adminPass', '')))

    def _require_auth(self):
        if not self._authed():
            self._send_json({'error': '管理密码不正确或无权限'}, 403)
            return False
        return True

    # ---------- GET ----------
    def do_GET(self):
        path = urlparse(self.path).path
        if path == '/api/ping':
            return self._send_json({'ok': True, 'mode': 'server', 'ts': int(time.time())})
        if path == '/api/config':
            return self._send_json(get_config())
        if path == '/api/state':
            if not self._require_auth():
                return
            return self._handle_state()
        if path == '/api/events':
            return self._handle_events()
        if path in ('/api/export/winners.csv', '/api/export/guests.csv'):
            if not self._require_auth():
                return
            return self._handle_export('winners' if 'winners' in path else 'guests')
        return super().do_GET()

    # ---------- POST ----------
    def do_POST(self):
        path = urlparse(self.path).path
        body = self._json_body()
        if body is None and path != '/api/events':
            return self._send_json({'error': '请求体不是合法 JSON'}, 400)

        if path == '/api/auth':
            ok = secrets.compare_digest(str(body.get('password', '')),
                                        str(get_config().get('adminPass', '')))
            return self._send_json({'ok': ok}, 200 if ok else 403)
        if path == '/api/lookup':
            return self._handle_lookup(body)
        if path == '/api/register':
            return self._handle_register(body)
        if path == '/api/config':
            if not self._require_auth():
                return
            return self._handle_save_config(body)
        if path == '/api/draw':
            if not self._require_auth():
                return
            return self._handle_draw(body)
        if path == '/api/winner-status':
            if not self._require_auth():
                return
            return self._handle_winner_status(body)
        if path == '/api/command':
            if not self._require_auth():
                return
            msg = dict(body)
            msg.pop('password', None)
            msg['_id'] = ('srv_%x_%s' % (int(time.time()), secrets.token_hex(3)))
            broadcast(msg)
            return self._send_json({'ok': True})
        if path == '/api/reset':
            if not self._require_auth():
                return
            return self._handle_reset()
        return self._send_json({'error': '未知接口'}, 404)

    # ---------- 各接口实现 ----------
    def _handle_lookup(self, body):
        fp = str(body.get('fp', '')).strip()
        if not fp:
            return self._send_json({'guest': None})
        with DB_LOCK:
            r = CONN.execute('SELECT * FROM guests WHERE fp=?', (fp,)).fetchone()
        return self._send_json({'guest': row_to_guest(r) if r else None})

    def _handle_register(self, body):
        name = str(body.get('name', '')).strip()
        phone = str(body.get('phone', '')).strip()
        wish = str(body.get('wish', '')).strip()[:60]
        fp = str(body.get('fp', '')).strip()
        if not name:
            return self._send_json({'error': '请填写您的姓名'}, 400)
        if len(name) > 12:
            return self._send_json({'error': '姓名最多 12 个字'}, 400)
        if not (len(phone) == 11 and phone.startswith('1') and phone.isdigit()):
            return self._send_json({'error': '请填写 11 位手机号'}, 400)

        with DB_LOCK:
            # 同一设备重复扫码 → 返回原券
            if fp:
                r = CONN.execute('SELECT * FROM guests WHERE fp=?', (fp,)).fetchone()
                if r:
                    return self._send_json({'guest': row_to_guest(r), 'existed': True})
            # 手机号防代领
            r = CONN.execute('SELECT * FROM guests WHERE phone=?', (phone,)).fetchone()
            if r:
                return self._send_json(
                    {'error': '该手机号已领取过号码券（No.%s）' % r['num']}, 409)
            # 事务内发号：并发登记也不会撞号
            try:
                CONN.execute('BEGIN IMMEDIATE')
                nxt = int(get_meta('next_num'))
                guest = {'num': pad3(nxt), 'name': name, 'phone': phone,
                         'wish': wish, 'code': rand_code(4), 'ts': int(time.time() * 1000)}
                CONN.execute(
                    'INSERT INTO guests(num,name,phone,wish,fp,code,ts) VALUES(?,?,?,?,?,?,?)',
                    (guest['num'], name, phone, wish, fp, guest['code'], guest['ts']))
                set_meta('next_num', str(nxt + 1))
                CONN.commit()
            except sqlite3.IntegrityError:   # 极端并发下的唯一约束兜底
                CONN.rollback()
                return self._send_json({'error': '登记冲突，请再试一次'}, 409)
            except Exception:
                CONN.rollback()
                raise
            backup()
        broadcast({'t': 'data', 'key': 'guests', '_id': ('srv_%x' % int(time.time()))})
        return self._send_json({'guest': guest, 'existed': False})

    def _handle_state(self):
        with DB_LOCK:
            guests_rows = CONN.execute('SELECT * FROM guests ORDER BY id').fetchall()
            guests_n = len(guests_rows)
            pool = pool_guests()
            winners = [dict(r) for r in CONN.execute(
                'SELECT * FROM winners ORDER BY ts')]
        drawn = {}
        for w in winners:
            if w['status'] != 'void':
                drawn[w['prize_id']] = drawn.get(w['prize_id'], 0) + 1
        # 字段名与前端对齐
        wl = [{'id': w['id'], 'prizeId': w['prize_id'], 'prizeName': w['prize_name'],
               'prizeDesc': w['prize_desc'] or '', 'round': w['round_label'] or '',
               'num': w['num'], 'name': w['name'], 'phone': w['phone'],
               'code': w['code'], 'ts': w['ts'], 'status': w['status']} for w in winners]
        return self._send_json({'guests': guests_n, 'pool': len(pool),
                                'poolNames': [g['name'] for g in pool],
                                'guestList': [{'num': r['num'], 'name': r['name'],
                                               'phone': r['phone'], 'wish': r['wish'] or '',
                                               'ts': r['ts']} for r in guests_rows],
                                'winners': wl, 'prizeDrawn': drawn,
                                'config': get_config()})

    def _handle_draw(self, body):
        prize_id = str(body.get('prizeId', ''))
        count = max(1, min(int(body.get('count', 1) or 1), 8))
        round_label = str(body.get('round', ''))
        cfg = get_config()
        prize = next((p for p in cfg['prizes'] if p['id'] == prize_id), None)
        if not prize:
            return self._send_json({'error': '奖项不存在'}, 400)

        with DB_LOCK:
            try:
                CONN.execute('BEGIN IMMEDIATE')
                pool = pool_guests()
                if len(pool) < count:
                    CONN.rollback()
                    return self._send_json(
                        {'error': '号码池人数不足（当前 %d 人）' % len(pool)}, 409)
                # secrets 系统级随机，绝无重复
                picks = secrets.SystemRandom().sample(pool, count)
                now = int(time.time() * 1000)
                ws = []
                for i, g in enumerate(picks):
                    w = {'id': 'w%x_%s' % (now, secrets.token_hex(3)),
                         'prizeId': prize['id'], 'prizeName': prize['name'],
                         'prizeDesc': prize.get('desc', ''), 'round': round_label,
                         'num': g['num'], 'name': g['name'], 'phone': g['phone'],
                         'code': g['code'], 'ts': now + i, 'status': 'win'}
                    CONN.execute(
                        '''INSERT INTO winners(id,prize_id,prize_name,prize_desc,
                           round_label,num,name,phone,code,ts,status)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                        (w['id'], w['prizeId'], w['prizeName'], w['prizeDesc'],
                         round_label, w['num'], w['name'], w['phone'], w['code'],
                         w['ts'], 'win'))
                    ws.append(w)
                CONN.commit()
            except Exception:
                CONN.rollback()
                raise
            backup()
        broadcast({'t': 'stop', 'prizeId': prize['id'], 'prizeName': prize['name'],
                   'prizeDesc': prize.get('desc', ''), 'round': round_label,
                   'count': count,
                   'winners': [{'num': w['num'], 'name': w['name']} for w in ws],
                   '_id': ('srv_%x_%s' % (int(time.time()), secrets.token_hex(3)))})
        return self._send_json({'winners': ws})

    def _handle_winner_status(self, body):
        wid = str(body.get('id', ''))
        status = str(body.get('status', ''))
        if status not in ('win', 'claimed', 'void'):
            return self._send_json({'error': '非法状态'}, 400)
        with DB_LOCK:
            cur = CONN.execute('UPDATE winners SET status=? WHERE id=?', (status, wid))
            CONN.commit()
            if cur.rowcount == 0:
                return self._send_json({'error': '记录不存在'}, 404)
            backup()
        broadcast({'t': 'data', 'key': 'winners', '_id': ('srv_%x' % int(time.time()))})
        return self._send_json({'ok': True})

    def _handle_save_config(self, body):
        cfg = get_config()
        for k in ('groom', 'bride', 'groomEn', 'brideEn', 'date', 'venue', 'title'):
            if k in body:
                cfg[k] = str(body[k]).strip()
        if 'adminPass' in body and str(body['adminPass']).strip():
            cfg['adminPass'] = str(body['adminPass']).strip()
        if isinstance(body.get('prizes'), list) and body['prizes']:
            ps = []
            for p in body['prizes']:
                if not str(p.get('name', '')).strip():
                    continue
                ps.append({'id': str(p.get('id') or 'p_' + secrets.token_hex(3)),
                           'name': str(p['name']).strip()[:12],
                           'count': max(1, int(p.get('count', 1) or 1)),
                           'perRound': max(1, min(int(p.get('perRound', 1) or 1), 8)),
                           'desc': str(p.get('desc', ''))[:40]})
            if not ps:
                return self._send_json({'error': '请至少保留一个奖项'}, 400)
            cfg['prizes'] = ps
        with DB_LOCK:
            set_meta('config', json.dumps(cfg, ensure_ascii=False))
            backup()
        broadcast({'t': 'data', 'key': 'config', '_id': ('srv_%x' % int(time.time()))})
        return self._send_json({'ok': True})

    def _handle_reset(self):
        with DB_LOCK:
            CONN.execute('DELETE FROM winners')
            CONN.execute('DELETE FROM guests')
            set_meta('next_num', '1')
            CONN.commit()
            backup()
        broadcast({'t': 'data', 'key': 'reset', '_id': ('srv_%x' % int(time.time()))})
        return self._send_json({'ok': True})

    def _handle_export(self, kind):
        buf = io.StringIO()
        w = csv.writer(buf)
        with DB_LOCK:
            if kind == 'winners':
                w.writerow(['时间', '奖项', '奖品', '号码', '姓名', '手机号', '防伪码', '状态'])
                st = {'win': '中奖', 'claimed': '已兑奖', 'void': '已弃奖'}
                for r in CONN.execute('SELECT * FROM winners ORDER BY ts'):
                    w.writerow([time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts'] / 1000)),
                                r['prize_name'], r['prize_desc'] or '', r['num'], r['name'],
                                r['phone'], r['code'], st.get(r['status'], r['status'])])
            else:
                w.writerow(['序号', '号码', '姓名', '手机号', '祝福语', '登记时间'])
                for i, r in enumerate(CONN.execute('SELECT * FROM guests ORDER BY id'), 1):
                    w.writerow([i, r['num'], r['name'], r['phone'], r['wish'] or '',
                                time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r['ts'] / 1000))])
        data = ('﻿' + buf.getvalue()).encode('utf-8')  # BOM，Excel 直开不乱码
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv; charset=utf-8')
        fname = '中奖记录' if kind == 'winners' else '到场名单'
        from urllib.parse import quote
        self.send_header('Content-Disposition',
                         'attachment; filename="%s.csv"; filename*=UTF-8\'\'%s'
                         % ('winners' if kind == 'winners' else 'guests', quote(fname + '.csv')))
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_events(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream; charset=utf-8')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('X-Accel-Buffering', 'no')   # 兼容 nginx 反代
        self.end_headers()
        q = queue.Queue(maxsize=100)
        with SUBS_LOCK:
            SUBS.append(q)
        print('[%s] 大屏/页面接入事件流（当前 %d 个订阅）' % (time.strftime('%H:%M:%S'), len(SUBS)))
        try:
            self.wfile.write(b'retry: 2000\n\n')
            self.wfile.flush()
            while True:
                try:
                    msg = q.get(timeout=25)
                    payload = json.dumps(msg, ensure_ascii=False).encode('utf-8')
                    self.wfile.write(b'data: ' + payload + b'\n\n')
                except queue.Empty:
                    self.wfile.write(b': hb\n\n')     # 心跳保活
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass
        finally:
            with SUBS_LOCK:
                if q in SUBS:
                    SUBS.remove(q)


class Server(ThreadingHTTPServer):
    daemon_threads = True   # SSE 长连接线程不阻塞退出
    allow_reuse_address = True


def main():
    ap = argparse.ArgumentParser(description='婚礼抽奖后端服务（零依赖）')
    ap.add_argument('--port', type=int, default=8000)
    ap.add_argument('--host', default='0.0.0.0')
    args = ap.parse_args()

    init_db()
    srv = Server((args.host, args.port), Handler)
    print('=' * 56)
    print('  婚礼抽奖服务已启动（数据文件：data.db + data_backup.json）')
    print('  本机访问：http://127.0.0.1:%d/' % args.port)
    print('  宾客领券：http://<服务器IP>:%d/register.html' % args.port)
    print('  抽奖大屏：http://<服务器IP>:%d/screen.html' % args.port)
    print('  控  制  台：http://<服务器IP>:%d/admin.html （密码默认 hn2026）')
    print('  Ctrl+C 停止服务（数据已落盘，重启不丢）')
    print('=' * 56)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\n已停止。数据保存在 data.db / data_backup.json')


if __name__ == '__main__':
    main()
