#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
STOCKSONAR 파이프라인
GlobeNewswire RSS 수집 → 필터링 → yfinance 시세 → Claude AI 분석 → docs/data.json 갱신 (+텔레그램 알림)

GitHub Actions에서 20분마다 자동 실행되도록 설계됨. 로컬 실행도 가능:
  ANTHROPIC_API_KEY=sk-... python pipeline.py

환경변수:
  ANTHROPIC_API_KEY   (필수) Claude API 키
  MODEL               (선택) 기본 claude-haiku-4-5-20251001
  ALERT_MIN_SCORE     (선택) 텔레그램 알림 기준 점수, 기본 70
  TELEGRAM_BOT_TOKEN  (선택) 텔레그램 봇 토큰
  TELEGRAM_CHAT_ID    (선택) 텔레그램 채팅/채널 ID
  MAX_NEW_PER_RUN     (선택) 1회 실행당 최대 분석 건수(비용 가드), 기본 20
  SONAR_MOCK          (선택) 1이면 네트워크/API 없이 모의 데이터로 전체 흐름 테스트
"""
import os, re, json, hashlib, time, html, sys
from datetime import datetime, timezone, timedelta

MOCK = os.environ.get("SONAR_MOCK") == "1"

RSS_URL = "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies"
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "docs", "data.json")
MODEL = os.environ.get("MODEL", "claude-haiku-4-5-20251001")
ALERT_MIN_SCORE = int(os.environ.get("ALERT_MIN_SCORE", "70"))
MAX_NEW_PER_RUN = int(os.environ.get("MAX_NEW_PER_RUN", "20"))
KEEP_DAYS = 30          # 뉴스 보관 기간
MAX_ITEMS = 400         # data.json 최대 보관 건수
MAX_SEEN = 2000         # 중복 방지 해시 보관 수

# ── 미국 정규 거래소만 허용 (비상장/OTC 제외) ─────────────────────────────
EXCH_MAP = {"NMS": "NASDAQ", "NGM": "NASDAQ", "NCM": "NASDAQ",
            "NYQ": "NYSE", "ASE": "NYSE AM", "PCX": "NYSE ARCA"}

# ── 쓸모없는 뉴스 필터 (법률 소송·주주 소송 권유 등) ──────────────────────
SPAM_KEYWORDS = [
    "class action", "investigation", "law firm", "lawsuit", "shareholder alert",
    "investor alert", "lead plaintiff", "deadline reminder", "securities fraud",
    "rosen law", "pomerantz", "glancy", "bronstein", "kahn swick", "levi & korsinsky",
    "schall law", "gross law", "kessler topaz", "hagens berman", "faruqi",
    "robbins geller", "johnson fistel", "wolf haldenstein", "kirby mcinerney",
    "total voting rights", "annual general meeting",
]

TICKER_RE = re.compile(
    r"\(\s*(?:NASDAQ|Nasdaq|NYSE\s*American|NYSE\s*Amer\.?|NYSE|NYSEAMERICAN|AMEX|CBOE)\s*[:\-]\s*([A-Z]{1,5})(?:\.[A-Z])?\s*\)"
)


def log(msg):
    print(f"[sonar] {msg}", flush=True)


def now_utc():
    return datetime.now(timezone.utc)


def h(s):
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]


# ════════════════════════════════ 1. 데이터 로드/저장 ════════════════════
def load_data():
    try:
        with open(DATA_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"updated": None, "fx": 1450.0, "items": [], "seen": []}


def save_data(data):
    os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
    with open(DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    log(f"data.json 저장 완료 — {len(data['items'])}건")


# ════════════════════════════════ 2. RSS 수집 ════════════════════════════
MOCK_ENTRIES = [
    {"title": "Acme Quantum (NASDAQ: ACMQ) Announces $25 Million AI Data Center Contract With Sovereign Fund",
     "summary": "Acme Quantum, a low float technology company, announced a binding agreement...",
     "link": "https://example.com/acmq", "published": now_utc().isoformat()},
    {"title": "Rosen Law Firm Reminds Investors of Class Action Against BigCo (NYSE: BIGC)",
     "summary": "lead plaintiff deadline...", "link": "https://example.com/spam",
     "published": now_utc().isoformat()},
    {"title": "Beta Bio (NASDAQ: BTBO) Prices $50 Million Public Offering of Common Stock",
     "summary": "Beta Bio announced the pricing of its underwritten public offering...",
     "link": "https://example.com/bbio", "published": now_utc().isoformat()},
    {"title": "Private Startup Announces New Product Line",
     "summary": "no ticker here", "link": "https://example.com/notk",
     "published": now_utc().isoformat()},
]


def fetch_rss():
    if MOCK:
        log("MOCK: RSS 모의 데이터 사용")
        return MOCK_ENTRIES
    import feedparser
    feed = feedparser.parse(RSS_URL, agent="Mozilla/5.0 (StockSonar; +personal project)")
    out = []
    for e in feed.entries:
        pub = None
        if getattr(e, "published_parsed", None):
            pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc).isoformat()
        out.append({"title": e.get("title", ""), "summary": e.get("summary", ""),
                    "link": e.get("link", ""), "published": pub or now_utc().isoformat()})
    log(f"RSS {len(out)}건 수신")
    return out


def extract_ticker(entry):
    text = entry["title"] + " " + entry["summary"]
    m = TICKER_RE.search(html.unescape(text))
    return m.group(1) if m else None


def is_spam(entry):
    t = (entry["title"] + " " + entry["summary"]).lower()
    return any(k in t for k in SPAM_KEYWORDS)


# ════════════════════════════════ 3. 시세 (yfinance) ═════════════════════
def get_quote(tk):
    if MOCK:
        return {"price": 3.21, "chg": 12.5, "mcapM": 45.0, "sharesM": 12.0,
                "exch": "NASDAQ", "name": f"{tk} Inc (mock)"}
    import yfinance as yf
    try:
        info = yf.Ticker(tk).info
        exch = EXCH_MAP.get(info.get("exchange", ""))
        if not exch:
            return None  # OTC/비상장 등 제외
        price = info.get("regularMarketPrice") or info.get("currentPrice")
        if not price:
            return None
        chg = info.get("regularMarketChangePercent")
        mcap = info.get("marketCap") or 0
        flt = info.get("floatShares") or info.get("sharesOutstanding") or 0
        return {"price": round(float(price), 4),
                "chg": round(float(chg), 2) if chg is not None else None,
                "mcapM": round(mcap / 1e6, 2),
                "sharesM": round(flt / 1e6, 2),
                "exch": exch,
                "name": info.get("shortName") or info.get("longName") or tk}
    except Exception as ex:
        log(f"yfinance 실패 {tk}: {ex}")
        return None


def get_fx(prev):
    if MOCK:
        return 1450.0
    import yfinance as yf
    try:
        v = yf.Ticker("USDKRW=X").info.get("regularMarketPrice")
        return round(float(v), 2) if v else prev
    except Exception:
        return prev


# ════════════════════════════════ 4. Claude AI 분석 ══════════════════════
SYSTEM_PROMPT = """너는 미국 주식 단기 트레이딩 뉴스 분석가다. 영문 보도자료를 분석해 아래 JSON만 출력한다(설명 금지).

{"ko":"한국어 제목(간결, 핵심 수치 포함)",
 "sum":"한국어 요약 2~3문장. 무엇이 발표됐고 왜 주가에 중요한지.",
 "sent":"good|bad|neut",
 "score":0~100 정수,
 "tags":[["+요인","p"] 또는 ["−요인","m"] 형식 3~5개],
 "cmt":"트레이더 관점 코멘트 1~2문장. 기회와 리스크를 균형있게."}

score(SONAR 점수) 산정 기준:
- 촉매 강도(최대 40): 인수합병·대형계약·FDA승인 35~40 / 정부프로그램·파트너십 25~35 / 임상데이터 20~30 / 학회발표·전임상 15~25 / 홍보성PR 5~15
- 수급 구조(최대 30): 유통주식 5M 미만 25~30 / 25M 미만 15~25 / 100M 미만 5~15 / 그 이상 0~5
- 가격대(최대 15): $0.5~$10 사이 10~15 / $10~$30 5~10 / 그 외 0~5
- 모멘텀(최대 15): 당일 +10% 이상 10~15 / 상승 중 5~10 / 하락 중 0~5
- 페널티: 신주발행·전환사채·희석 −20~−40 / 소송·규제 −20~−30 / 구체성 없는 PR −10~−20
악재(공모·희석·소송 등)는 sent="bad"로, 점수는 낮게. 판단 불가·홍보성은 "neut"."""


def analyze(entry, tk, quote):
    if MOCK:
        bad = "offering" in entry["title"].lower()
        return {"ko": f"[모의] {tk} 분석 제목", "sum": "모의 요약입니다.",
                "sent": "bad" if bad else "good", "score": 25 if bad else 80,
                "tags": [["+모의 태그", "p"], ["−모의 리스크", "m"]], "cmt": "모의 코멘트."}
    from anthropic import Anthropic
    client = Anthropic()
    ctx = (f"제목: {entry['title']}\n본문 요약: {entry['summary'][:1500]}\n"
           f"종목: {tk} ({quote.get('exch')}) 주가 ${quote['price']}, "
           f"당일등락 {quote.get('chg')}%, 시총 {quote['mcapM']}M달러, 유통주식 {quote['sharesM']}M주")
    msg = client.messages.create(model=MODEL, max_tokens=800, temperature=0.2,
                                 system=SYSTEM_PROMPT,
                                 messages=[{"role": "user", "content": ctx}])
    text = msg.content[0].text
    m = re.search(r"\{.*\}", text, re.S)
    out = json.loads(m.group(0))
    out["score"] = max(0, min(100, int(out.get("score", 50))))
    if out.get("sent") not in ("good", "bad", "neut"):
        out["sent"] = "neut"
    out["tags"] = [[str(t[0])[:30], "m" if (len(t) > 1 and t[1] == "m") else "p"]
                   for t in (out.get("tags") or [])][:5]
    return out


# ════════════════════════════════ 5. 텔레그램 알림 ═══════════════════════
def telegram_alert(item):
    tok, chat = os.environ.get("TELEGRAM_BOT_TOKEN"), os.environ.get("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        return
    if MOCK:
        log(f"MOCK: 텔레그램 알림 — {item['tk']} {item['score']}점")
        return
    import requests
    q = item["q"]
    text = (f"📡 <b>SONAR {item['score']}</b> — <b>${item['tk']}</b> "
            f"(${q['price']}, {'+' if (q.get('chg') or 0) >= 0 else ''}{q.get('chg')}%)\n"
            f"{item['ko']}\n💡 {item['cmt']}\n{item.get('url','')}")
    try:
        requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                      json={"chat_id": chat, "text": text, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=10)
    except Exception as ex:
        log(f"텔레그램 실패: {ex}")


# ════════════════════════════════ 메인 ═══════════════════════════════════
def main():
    data = load_data()
    seen = set(data.get("seen", []))
    entries = fetch_rss()

    new_items, analyzed = [], 0
    for e in entries:
        key = h(e["link"] or e["title"])
        if key in seen:
            continue
        seen.add(key)                      # 본 뉴스는 결과와 무관하게 기록(재시도 낭비 방지)
        if is_spam(e):
            log(f"스팸 제외: {e['title'][:60]}")
            continue
        tk = extract_ticker(e)
        if not tk:
            continue
        if analyzed >= MAX_NEW_PER_RUN:
            log("이번 실행 분석 한도 도달 — 다음 실행에서 계속")
            break
        quote = get_quote(tk)
        if not quote:
            log(f"시세 없음/비정규거래소 제외: {tk}")
            continue
        try:
            ai = analyze(e, tk, quote)
        except Exception as ex:
            log(f"AI 분석 실패 {tk}: {ex}")
            continue
        analyzed += 1
        item = {"tk": tk, "name": quote.pop("name", tk), "time": e["published"],
                "url": e["link"], "en": e["title"], "q": quote, **ai}
        new_items.append(item)
        log(f"분석 완료: {tk} score={ai['score']} {ai['sent']}")
        if ai["score"] >= ALERT_MIN_SCORE and ai["sent"] == "good":
            telegram_alert(item)
        time.sleep(0.5)                    # API 예의상 간격

    # 병합: 새 항목 앞에, 오래된 항목 제거
    cutoff = (now_utc() - timedelta(days=KEEP_DAYS)).isoformat()
    items = new_items + [i for i in data["items"] if i.get("time", "") >= cutoff]
    data["items"] = items[:MAX_ITEMS]
    data["seen"] = list(seen)[-MAX_SEEN:]
    data["fx"] = get_fx(data.get("fx", 1450.0))
    data["updated"] = now_utc().isoformat()
    save_data(data)
    log(f"완료 — 신규 {len(new_items)}건 / 전체 {len(data['items'])}건")


if __name__ == "__main__":
    main()
