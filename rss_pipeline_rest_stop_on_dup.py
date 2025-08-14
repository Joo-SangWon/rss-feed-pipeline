import os, json, time
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import requests
import feedparser
import pymysql
from rapidfuzz import process, fuzz

# =========================
# 설정
# =========================
DB_HOST = "192.168.0.198"
DB_PORT = 3306
DB_USER = "stockAdm"
DB_PASSWORD = "09stockAdm1@"
DB_NAME = "kstock"
DB_CHARSET = "utf8mb4"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o-mini"

RSS_FEEDS = {
    '연합뉴스_경제': 'https://www.yna.co.kr/rss/economy.xml',
    '연합뉴스_사회': 'https://www.yna.co.kr/rss/society.xml',
    '매일경제_경제': 'https://www.mk.co.kr/rss/30100041/',
    '매일경제_사회': 'https://www.mk.co.kr/rss/30100029/',
    '경향신문_경제': 'https://www.khan.co.kr/rss/rssdata/economy_news.xml',
    '경향신문_사회': 'https://www.khan.co.kr/rss/rssdata/society_news.xml',
    '한국경제_마켓': 'https://www.hankyung.com/feed/market',
    '한국경제_사회': 'https://www.hankyung.com/feed/society',
    '동아일보_경제': 'https://rss.donga.com/economy.xml',
    '동아일보_사회': 'https://rss.donga.com/national.xml',
}

# =========================
# DB 유틸
# =========================
def get_conn():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME, charset=DB_CHARSET, autocommit=True
    )

def create_table(conn):
    """
    새로 만드는 환경이면 아래 스키마 사용 (1~5 종목 컬럼 포함)
    기존 환경이면 다음 마이그레이션을 먼저 실행하세요:
      ALTER TABLE rss
        CHANGE stock_code stock_code1 VARCHAR(10) NULL,
        CHANGE stock_name stock_name1 VARCHAR(50) NULL;
      ALTER TABLE rss
        ADD COLUMN stock_code2 VARCHAR(10) NULL AFTER stock_name1,
        ADD COLUMN stock_name2 VARCHAR(50) NULL AFTER stock_code2,
        ADD COLUMN stock_code3 VARCHAR(10) NULL AFTER stock_name2,
        ADD COLUMN stock_name3 VARCHAR(50) NULL AFTER stock_code3,
        ADD COLUMN stock_code4 VARCHAR(10) NULL AFTER stock_name3,
        ADD COLUMN stock_name4 VARCHAR(50) NULL AFTER stock_code4,
        ADD COLUMN stock_code5 VARCHAR(10) NULL AFTER stock_name4,
        ADD COLUMN stock_name5 VARCHAR(50) NULL AFTER stock_code5;
    """
    sql = """
    CREATE TABLE IF NOT EXISTS rss (
        id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(50),
        title TEXT,
        link VARCHAR(500) UNIQUE,
        pub_date DATETIME,
        summary TEXT,
        stock_code1 VARCHAR(10),
        stock_name1 VARCHAR(50),
        stock_code2 VARCHAR(10),
        stock_name2 VARCHAR(50),
        stock_code3 VARCHAR(10),
        stock_name3 VARCHAR(50),
        stock_code4 VARCHAR(10),
        stock_name4 VARCHAR(50),
        stock_code5 VARCHAR(10),
        stock_name5 VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_pub_date (pub_date)
    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """
    with conn.cursor() as cur:
        cur.execute(sql)

# =========================
# 도우미
# =========================
_TRACKING_PARAMS_PREFIX = ("utm_", )
_TRACKING_PARAMS_EXACT = {"gclid", "fbclid", "msclkid", "ref", "referrer"}

def normalize_link(url: str) -> str:
    if not url:
        return url
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    fragment = ""
    q = []
    for k, v in parse_qsl(parts.query, keep_blank_values=True):
        if k.startswith(_TRACKING_PARAMS_PREFIX) or k in _TRACKING_PARAMS_EXACT:
            continue
        q.append((k, v))
    query = urlencode(q, doseq=True)
    path = parts.path or "/"
    return urlunsplit((scheme, netloc, path, query, fragment))

def to_datetime_safe(entry):
    try:
        if hasattr(entry, "published_parsed") and entry.published_parsed:
            return datetime(*entry.published_parsed[:6])
    except Exception:
        pass
    return None

# =========================
# stock_master
# =========================
def load_stock_master(conn):
    sql = "SELECT stock_code, stock_name FROM stock_master"
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    master = []
    for code, name in rows:
        master.append({"code": code, "name": name, "aliases": [name]})
    return master

def build_name_index(master):
    idx = []
    for m in master:
        idx.extend(m["aliases"])
    return idx

# =========================
# LLM (REST)
# =========================
def call_llm_companies_rest(title: str, summary: str, timeout=15) -> list[str]:
    """
    최대 5개 기업명을 JSON 배열로만 반환. 형식 오류 시 [].
    """
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY 미설정")
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    prompt = f"""
당신은 한국 뉴스에서 기업명을 식별하는 어시스턴트입니다.
아래 기사 제목과 요약을 보고, 관련된 한국 상장사(기업)명을 **최대 5개까지** JSON 배열로만 출력하세요.
불명확하면 빈 배열([])을 출력하세요. 다른 말은 쓰지 마세요.

제목: {title}
요약: {summary}

출력 예(있을 때): ["삼성전자", "카카오", "네이버"]
출력 예(없을 때): []
"""
    payload = {"model": OPENAI_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
    r.raise_for_status()
    content = r.json()["choices"][0]["message"]["content"].strip()
    try:
        data = json.loads(content)
        return [str(x) for x in data][:5] if isinstance(data, list) else []
    except Exception:
        return []

# =========================
# 매핑 (여러 종목)
# =========================
def map_name_to_master(company_name: str, name_index, master, threshold: int = 85):
    if not company_name:
        return None
    best = process.extractOne(company_name, name_index, scorer=fuzz.WRatio)
    if not best:
        return None
    match, score, idx = best
    if score < threshold:
        return None
    for m in master:
        if match in m["aliases"]:
            return {"stock_code": m["code"], "stock_name": m["name"], "score": score}
    return None

def select_top_unique_mappings(candidates, name_index, master, limit=5):
    mapped = []
    seen_codes = set()
    for c in candidates:
        res = map_name_to_master(c, name_index, master)
        if not res:
            continue
        code = res["stock_code"]
        if code in seen_codes:
            continue
        seen_codes.add(code)
        mapped.append(res)
    mapped.sort(key=lambda x: x["score"], reverse=True)
    return mapped[:limit]

# =========================
# DB 조작
# =========================
def insert_shell(cur, source, title, link, pub_date, summary):
    """
    껍데기 insert (매핑 없이). 중복이면 False 반환 → 해당 피드 중단
    """
    cur.execute(
        """
        INSERT IGNORE INTO rss (
            source, title, link, pub_date, summary,
            stock_code1, stock_name1, stock_code2, stock_name2,
            stock_code3, stock_name3, stock_code4, stock_name4,
            stock_code5, stock_name5
        )
        VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
        (source, title, link, pub_date, summary)
    )
    return cur.rowcount > 0

def update_mapping_multi(cur, link, mappings):
    """
    mappings: [{"stock_code":..., "stock_name":...}, ...] 최대 5개
    """
    codes = [m["stock_code"] for m in mappings] + [None]*5
    names = [m["stock_name"] for m in mappings] + [None]*5
    codes = codes[:5]; names = names[:5]

    cur.execute(
        """
        UPDATE rss
        SET
          stock_code1=%s, stock_name1=%s,
          stock_code2=%s, stock_name2=%s,
          stock_code3=%s, stock_name3=%s,
          stock_code4=%s, stock_name4=%s,
          stock_code5=%s, stock_name5=%s
        WHERE link=%s
        """,
        (codes[0], names[0], codes[1], names[1], codes[2], names[2],
         codes[3], names[3], codes[4], names[4], link)
    )

# =========================
# 파이프라인 (중복 감지 시 해당 피드 중단)
# =========================
def process_feed(conn, master, name_index):
    stats = {
        "feeds_total": len(RSS_FEEDS),
        "feeds_stopped_on_dup": 0,
        "articles_seen": 0,
        "inserted": 0,
        "mapped_articles": 0,      # 1개 이상 매핑된 기사 수
        "mapped_pairs": 0,         # 총 매핑 건수(기사×종목)
        "saved_unmapped": 0,
        "llm_fail": 0,
    }

    with conn.cursor() as cur:
        for name, url in RSS_FEEDS.items():
            print(f"\n📡 {name} 수집 중... ({url})")
            feed = feedparser.parse(url)

            if getattr(feed, "bozo", False):
                print(f"⚠️ 파싱 경고: {getattr(feed, 'bozo_exception', '')}")

            stop_this_feed = False

            for entry in feed.entries:
                if stop_this_feed:
                    break

                title   = entry.get('title', '[제목 없음]')
                link    = normalize_link(entry.get('link', ''))
                pubdate = to_datetime_safe(entry)
                summary = entry.get('summary', '') or entry.get('description', '')

                stats["articles_seen"] += 1

                # 0) 저장 먼저 (중복이면 피드 중단)
                inserted = insert_shell(cur, name, title, link, pubdate, summary)
                if not inserted:
                    print(f"⏹ 중복 감지 → 피드 중단 | {title[:60]}")
                    stats["feeds_stopped_on_dup"] += 1
                    break

                stats["inserted"] += 1

                # 1) 신규만 LLM 매핑 (여러 종목)
                try:
                    cands = call_llm_companies_rest(title or "", summary or "")
                except Exception as e:
                    print(f"✅ 저장(LLM 실패, 미매핑) | {title[:60]} | 오류: {e}")
                    stats["llm_fail"] += 1
                    stats["saved_unmapped"] += 1
                    continue

                if not cands:
                    print(f"✅ 저장(미매핑) | {title[:60]} | 후보 없음")
                    stats["saved_unmapped"] += 1
                    continue

                mappings = select_top_unique_mappings(cands, name_index, master, limit=5)
                if not mappings:
                    print(f"✅ 저장(미매핑) | {title[:60]} | 후보: {cands}")
                    stats["saved_unmapped"] += 1
                    continue

                update_mapping_multi(cur, link, mappings)
                stats["mapped_articles"] += 1
                stats["mapped_pairs"] += len(mappings)

                tags = ", ".join([f"{m['stock_name']}({m['stock_code']})" for m in mappings])
                print(f"✅ 저장+매핑 [{tags}] | {title[:60]}")
                time.sleep(0.1)  # 과도호출 방지(옵션)

    return stats

# =========================
# 메인 (요약 출력)
# =========================
def main():
    conn = get_conn()
    try:
        create_table(conn)  # 새 환경이면 테이블 생성; 기존은 ALTER 이후에도 안전
        master = load_stock_master(conn)
        name_index = build_name_index(master)
        stats = process_feed(conn, master, name_index)

        print("\n==== 실행 요약 ====")
        print(f"• 피드 총수: {stats['feeds_total']}")
        print(f"• 중복으로 중단된 피드: {stats['feeds_stopped_on_dup']}")
        print(f"• 본 기사수(피드 내): {stats['articles_seen']}")
        print(f"• 신규 저장: {stats['inserted']}")
        print(f"• 매핑된 기사 수: {stats['mapped_articles']}")
        print(f"• 총 매핑 건수(기사×종목): {stats['mapped_pairs']}")
        print(f"• 미매핑 저장: {stats['saved_unmapped']} (LLM 실패 {stats['llm_fail']} 포함)")
        print("====================\n")

        return stats
    finally:
        conn.close()

if __name__ == "__main__":
    main()
