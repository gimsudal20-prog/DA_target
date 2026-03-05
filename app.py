# -*- coding: utf-8 -*-
"""
app.py (Open API 전용 통합 버전 + 스케줄 & 하루예산 일괄 업데이트 기능)
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
import time
import hmac
import hashlib
import base64
import requests
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATES_DIR)

OPENAPI_BASE_URL = "https://api.searchad.naver.com"

# -----------------------------
# Open API (HMAC) 공통 로직
# -----------------------------
def _sig(ts: str, method: str, uri: str, secret_key: str) -> str:
    msg = f"{ts}.{method.upper()}.{uri}"
    dig = hmac.new(str(secret_key).strip().encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(dig).decode()

def _open_headers(api_key: str, secret_key: str, customer_id: str, method: str, uri: str) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "X-Timestamp": ts,
        "X-API-KEY": str(api_key).strip(),
        "X-Customer": str(customer_id).strip(),
        "X-Signature": _sig(ts, method, uri, secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }

def open_get(api_key, secret_key, customer_id, uri: str, params=None):
    return requests.get(
        OPENAPI_BASE_URL + uri,
        headers=_open_headers(api_key, secret_key, customer_id, "GET", uri),
        params=params,
    )

def open_put(api_key, secret_key, customer_id, uri: str, params=None, json_body=None):
    return requests.put(
        OPENAPI_BASE_URL + uri,
        headers=_open_headers(api_key, secret_key, customer_id, "PUT", uri),
        params=params,
        json=json_body,
    )

DAY_NUM_TO_CODE = {1: "MON", 2: "TUE", 3: "WED", 4: "THU", 5: "FRI", 6: "SAT", 7: "SUN"}

# -----------------------------
# 페이지 렌더링
# -----------------------------
@app.route("/")
def index():
    accounts = []
    csv_path = os.path.join(BASE_DIR, "accounts.csv")
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(csv_path, encoding="cp949")

        cols = {c.lower().strip(): c for c in df.columns}
        cid_col = cols.get("customer_id") or cols.get("customerid") or cols.get("customer") or cols.get("custid")
        name_col = cols.get("account_name") or cols.get("accountname") or cols.get("name") or cols.get("account")

        if cid_col is None: cid_col = df.columns[0]
        if name_col is None: name_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]

        df2 = df[[cid_col, name_col]].copy()
        df2.columns = ["customer_id", "account_name"]
        df2["customer_id"] = df2["customer_id"].astype(str).str.strip()
        df2["account_name"] = df2["account_name"].astype(str).str.strip()
        accounts = df2.to_dict(orient="records")

    return render_template("index.html", accounts=accounts)

# -----------------------------
# API 라우트
# -----------------------------
@app.route("/get_campaigns", methods=["POST"])
def get_campaigns():
    d = request.json or {}
    cid = d.get("customer_id")
    res = open_get(d.get("api_key"), d.get("secret_key"), cid, "/ncc/campaigns")
    if res.status_code == 200:
        return jsonify(res.json())
    return jsonify({"error": "캠페인 조회 실패", "status": res.status_code, "details": res.text}), 400

@app.route("/get_adgroups", methods=["POST"])
def get_adgroups():
    d = request.json or {}
    cid = d.get("customer_id")
    camp = str(d.get("campaign_id") or "").strip()
    res = open_get(d.get("api_key"), d.get("secret_key"), cid, "/ncc/adgroups")
    if res.status_code == 200:
        all_g = res.json()
        return jsonify([g for g in all_g if str(g.get("nccCampaignId")) == camp])
    return jsonify({"error": "광고그룹 조회 실패", "status": res.status_code, "details": res.text}), 400


# ✅ 기능 1: 시간 타겟팅 & 가중치 일괄 처리
@app.route("/update_schedule", methods=["POST"])
def update_schedule():
    d = request.json or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    adgroup_ids = d.get("adgroup_ids", []) 

    start = int(d.get("start", 0))
    end = int(d.get("end", 24))
    days = d.get("days", [])
    bid_weight = int(d.get("bidWeight", 100))

    if not adgroup_ids:
        return jsonify({"error": "선택된 광고그룹이 없습니다."}), 400

    codes = []
    for d_num in days:
        day_str = DAY_NUM_TO_CODE[int(d_num)]
        for h in range(start, end):
            codes.append(f"SD{day_str}{h:02d}{(h+1):02d}")

    results = {"success": 0, "fail": 0, "details": []}

    for owner_id in adgroup_ids:
        owner_id = str(owner_id).strip()
        uri = f"/ncc/criterion/{owner_id}/SD"
        
        target_body = [
            {
                "customerId": int(cid),
                "ownerId": owner_id,
                "dictionaryCode": c, 
                "type": "SD"
            } 
            for c in codes
        ]

        r_target = open_put(api_key, secret_key, cid, uri, json_body=target_body)
        if r_target.status_code != 200:
            results["fail"] += 1
            results["details"].append({"id": owner_id, "error": f"타겟팅 실패: {r_target.text}"})
            continue 

        if codes: 
            chunk_size = 50
            bw_fail = False
            for i in range(0, len(codes), chunk_size):
                chunk = codes[i:i + chunk_size]
                bw_uri = f"/ncc/criterion/{owner_id}/bidWeight"
                r_bw_chunk = open_put(api_key, secret_key, cid, bw_uri, params={"codes": ",".join(chunk), "bidWeight": bid_weight})
                if r_bw_chunk.status_code != 200:
                    bw_fail = True
                    results["details"].append({"id": owner_id, "error": f"가중치 실패: {r_bw_chunk.text}"})
                    break
            
            if bw_fail:
                results["fail"] += 1
                continue

        results["success"] += 1

    return jsonify({
        "ok": True,
        "message": f"총 {len(adgroup_ids)}개 중 성공: {results['success']}개 / 실패: {results['fail']}개",
        "results": results,
        "applied_codes_count": len(codes),
        "bid_weight": bid_weight
    })

# ✅ 기능 2: 하루 예산 (캠페인/광고그룹) 일괄 업데이트 (실제 반영 버그 완벽 수정)
@app.route("/update_budget", methods=["POST"])
def update_budget():
    d = request.json or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    
    entity_type = d.get("entity_type") # "campaign" or "adgroup"
    entity_ids = d.get("entity_ids", [])
    budget = int(d.get("budget", 0))

    if not entity_ids:
        return jsonify({"error": "선택된 대상이 없습니다."}), 400

    use_daily_budget = True if budget > 0 else False
    daily_budget = budget if budget > 0 else 0

    results = {"success": 0, "fail": 0, "details": []}

    for eid in entity_ids:
        eid = str(eid).strip()
        if entity_type == "campaign":
            uri = f"/ncc/campaigns/{eid}"
        else:
            uri = f"/ncc/adgroups/{eid}"
            
        r_get = open_get(api_key, secret_key, cid, uri)
        if r_get.status_code != 200:
            results["fail"] += 1
            results["details"].append({"id": eid, "error": f"조회 실패: {r_get.text}"})
            continue
            
        obj = r_get.json()
        
        # 🚨 핵심 수정 부분: 객체의 최상단(Top-Level) 위치에 예산 변수를 직접 덮어씁니다.
        obj["useDailyBudget"] = use_daily_budget
        obj["dailyBudget"] = daily_budget

        # 혹시 이전 버전에서 잘못 삽입되었던 budget 속성이 있다면 안전하게 제거
        if "budget" in obj:
            del obj["budget"]

        # 파라미터는 ?fields=budget 으로 보내고, 바디에는 수정된 전체 객체(obj)를 전송
        r_put = open_put(api_key, secret_key, cid, uri, params={"fields": "budget"}, json_body=obj)
        if r_put.status_code == 200:
            results["success"] += 1
        else:
            results["fail"] += 1
            results["details"].append({"id": eid, "error": r_put.text})

    return jsonify({
        "ok": True,
        "message": f"총 {len(entity_ids)}개 예산 업데이트 성공: {results['success']}개 / 실패: {results['fail']}개",
        "results": results
    })

@app.route("/get_schedule", methods=["POST"])
def get_schedule():
    d = request.json or {}
    api_key = d.get("api_key")
    secret_key = d.get("secret_key")
    cid = d.get("customer_id")
    owner_id = str(d.get("adgroup_id") or "").strip()

    uri = f"/ncc/criterion/{owner_id}"
    r = open_get(api_key, secret_key, cid, uri, params={"type": "SD"})
    if r.status_code == 200:
        return jsonify(r.json())
    return jsonify({"error": "스케줄 조회 실패", "status": r.status_code, "details": r.text}), 400

if __name__ == "__main__":
    app.run(debug=True, port=5000)