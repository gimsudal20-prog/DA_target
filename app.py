# -*- coding: utf-8 -*-
"""
app.py (Open API 전용 통합 버전)
- 스케줄 일괄 변경 로직 업데이트: 단일 범위(start~end)에서 -> 다중 선택(hours list) 방식으로 변경
"""

from flask import Flask, render_template, request, jsonify
import pandas as pd
import time
import hmac
import hashlib
import base64
import requests
import os
import copy
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

app = Flask(__name__, template_folder=TEMPLATES_DIR)
OPENAPI_BASE_URL = "https://api.searchad.naver.com"

@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"error": f"서버 내부 오류: {str(e)}"}), 500

def _sig(ts: str, method: str, uri: str, secret_key: str) -> str:
    msg = f"{ts}.{method.upper()}.{uri}"
    dig = hmac.new(str(secret_key).strip().encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(dig).decode()

def _open_headers(api_key: str, secret_key: str, customer_id: str, method: str, uri: str) -> dict:
    ts = str(int(time.time() * 1000))
    return {
        "X-Timestamp": ts, "X-API-KEY": str(api_key).strip(),
        "X-Customer": str(customer_id).strip(), "X-Signature": _sig(ts, method, uri, secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }

def _do_req(method, api_key, secret_key, cid, uri, params=None, json_body=None, max_retries=3):
    url = OPENAPI_BASE_URL + uri
    for i in range(max_retries):
        headers = _open_headers(api_key, secret_key, cid, method, uri)
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=15)
            if r.status_code in [200, 201, 204]: return r
            if r.status_code == 429: time.sleep(1.5); continue
            if r.status_code == 404 and "1018" in r.text: time.sleep(1.0); continue
            return r
        except requests.exceptions.RequestException as e:
            time.sleep(1.5)
            if i == max_retries - 1:
                class FakeResponse: status_code = 500; text = f"네트워크 통신 실패: {str(e)}"
                return FakeResponse()

DAY_NUM_TO_CODE = {1: "MON", 2: "TUE", 3: "WED", 4: "THU", 5: "FRI", 6: "SAT", 7: "SUN"}

@app.route("/")
def index():
    accounts = []
    csv_path = os.path.join(BASE_DIR, "accounts.csv")
    if os.path.exists(csv_path):
        try: df = pd.read_csv(csv_path, encoding="utf-8-sig")
        except: df = pd.read_csv(csv_path, encoding="cp949")
        cols = {c.lower().strip(): c for c in df.columns}
        cid_col = cols.get("customer_id") or cols.get("customerid") or df.columns[0]
        name_col = cols.get("account_name") or cols.get("name") or (df.columns[1] if len(df.columns)>1 else df.columns[0])
        df2 = df[[cid_col, name_col]].copy()
        df2.columns = ["customer_id", "account_name"]
        df2["customer_id"] = df2["customer_id"].astype(str).str.strip()
        df2["account_name"] = df2["account_name"].astype(str).str.strip()
        accounts = df2.to_dict(orient="records")
    return render_template("index.html", accounts=accounts)

@app.route("/get_campaigns", methods=["POST"])
def get_campaigns():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/campaigns")
    if res.status_code == 200: return jsonify(res.json())
    return jsonify({"error": "캠페인 조회 실패", "details": res.text}), 400

@app.route("/get_adgroups", methods=["POST"])
def get_adgroups():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/adgroups", params={"nccCampaignId": d.get("campaign_id")})
    if res.status_code == 200: return jsonify(res.json())
    return jsonify({"error": "광고그룹 조회 실패", "details": res.text}), 400

@app.route("/get_biz_channels", methods=["POST"])
def get_biz_channels():
    d = request.json or {}
    res = _do_req("GET", d.get("api_key"), d.get("secret_key"), d.get("customer_id"), "/ncc/channels")
    if res.status_code == 200: return jsonify(res.json())
    return jsonify({"error": "비즈채널 조회 실패", "details": res.text}), 400

@app.route("/copy_campaigns", methods=["POST"])
def copy_campaigns():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    src_ids, suffix = d.get("source_ids", []), d.get("suffix", "_복사본")

    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/campaigns/{src_id}")
        if r_get.status_code != 200: results["fail"] += 1; continue
        
        src = r_get.json()
        new_camp = {
            "customerId": int(cid),
            "name": src.get("name", "") + suffix,
            "campaignTp": src.get("campaignTp"), 
            "useDailyBudget": src.get("useDailyBudget", False),
            "dailyBudget": src.get("dailyBudget", 0),
        }
        
        r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/campaigns", json_body=new_camp)
        if r_post.status_code in [200, 201]:
            results["success"] += 1
        else:
            results["fail"] += 1
            all_errors.append(f"[{new_camp['name']}] 생성 실패: {r_post.text}")

    msg = f"캠페인 복사 완료!\n(성공: {results['success']}개, 실패: {results['fail']}개)"
    if all_errors: msg += "\n" + "\n".join(all_errors[:5])
    return jsonify({"ok": True, "message": msg})

@app.route("/update_budget", methods=["POST"])
def update_budget():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids, budget = d.get("entity_type"), d.get("entity_ids", []), int(d.get("budget", 0))
    if not entity_ids: return jsonify({"error": "선택된 대상이 없습니다."}), 400
    results = {"success": 0, "fail": 0}
    for eid in entity_ids:
        uri = f"/ncc/campaigns/{eid.strip()}" if entity_type == "campaign" else f"/ncc/adgroups/{eid.strip()}"
        r_get = _do_req("GET", api_key, secret_key, cid, uri)
        if r_get.status_code != 200: results["fail"] += 1; continue
        obj = r_get.json()
        obj["useDailyBudget"], obj["dailyBudget"] = (budget > 0), budget
        if "budget" in obj: del obj["budget"]
        r_put = _do_req("PUT", api_key, secret_key, cid, uri, params={"fields": "budget"}, json_body=obj)
        if r_put.status_code == 200: results["success"] += 1
        else: results["fail"] += 1
    return jsonify({"ok": True, "message": f"총 {len(entity_ids)}개 예산 업데이트 성공: {results['success']}개 / 실패: {results['fail']}개"})

# 🔥 신규: 다중 시간 선택(hours list) 스케줄 처리
@app.route("/update_schedule", methods=["POST"])
def update_schedule():
    d = request.json or {}
    api_key, secret_key, cid, adgroup_ids = d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("adgroup_ids", [])
    days, hours, bid_weight = d.get("days",[]), d.get("hours",[]), int(d.get("bidWeight",100))
    
    # 전달받은 시간대(hours) 리스트를 기반으로 코드 생성
    codes = [f"SD{DAY_NUM_TO_CODE[int(d_num)]}{int(h):02d}{(int(h)+1):02d}" for d_num in days for h in hours]
    
    results = {"success": 0, "fail": 0}
    for owner_id in adgroup_ids:
        uri = f"/ncc/criterion/{owner_id.strip()}/SD"
        target_body = [{"customerId": int(cid), "ownerId": owner_id.strip(), "dictionaryCode": c, "type": "SD"} for c in codes]
        if _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body).status_code != 200: results["fail"] += 1; continue 
        if codes: 
            for i in range(0, len(codes), 50): 
                _do_req("PUT", api_key, secret_key, cid, f"/ncc/criterion/{owner_id.strip()}/bidWeight", params={"codes": ",".join(codes[i:i+50]), "bidWeight": bid_weight})
        results["success"] += 1
    return jsonify({"ok": True, "message": f"총 {len(adgroup_ids)}개 스케줄 업데이트 성공: {results['success']}개 / 실패: {results['fail']}개"})

@app.route("/update_schedule_campaign_bulk", methods=["POST"])
def update_schedule_campaign_bulk():
    d = request.json or {}
    api_key, secret_key, cid, campaign_ids = d.get("api_key"), d.get("secret_key"), d.get("customer_id"), d.get("campaign_ids", [])
    days, hours, bid_weight = d.get("days",[]), d.get("hours",[]), int(d.get("bidWeight",100))
    
    adgroup_ids = []
    for camp_id in campaign_ids:
        r_adgs = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": camp_id})
        if r_adgs.status_code == 200: adgroup_ids.extend([adg.get("nccAdgroupId") for adg in r_adgs.json()])
    
    codes = [f"SD{DAY_NUM_TO_CODE[int(d_num)]}{int(h):02d}{(int(h)+1):02d}" for d_num in days for h in hours]
    
    results = {"success": 0, "fail": 0}
    for owner_id in adgroup_ids:
        uri = f"/ncc/criterion/{owner_id}/SD"
        target_body = [{"customerId": int(cid), "ownerId": owner_id, "dictionaryCode": c, "type": "SD"} for c in codes]
        if _do_req("PUT", api_key, secret_key, cid, uri, json_body=target_body).status_code != 200: results["fail"] += 1; continue 
        if codes: 
            for i in range(0, len(codes), 50): 
                _do_req("PUT", api_key, secret_key, cid, f"/ncc/criterion/{owner_id}/bidWeight", params={"codes": ",".join(codes[i:i+50]), "bidWeight": bid_weight})
        results["success"] += 1
    return jsonify({"ok": True, "message": f"하위 광고그룹 총 {len(adgroup_ids)}개 스케줄 일괄 변경 완료!\n(성공: {results['success']} / 실패: {results['fail']})"})

@app.route("/update_keyword_bids", methods=["POST"])
def update_keyword_bids():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("secret_key"), d.get("customer_id")
    entity_type, entity_ids = d.get("entity_type"), d.get("entity_ids", [])
    bid_amt = int(d.get("bid_amt", 70))

    if not entity_ids: return jsonify({"error": "대상이 없습니다."}), 400

    adgroup_ids = []
    if entity_type == "campaign":
        for camp_id in entity_ids:
            r_adgs = _do_req("GET", api_key, secret_key, cid, "/ncc/adgroups", params={"nccCampaignId": camp_id})
            if r_adgs.status_code == 200:
                adgroup_ids.extend([adg.get("nccAdgroupId") for adg in r_adgs.json()])
    else:
        adgroup_ids = entity_ids

    use_group_bid = (bid_amt == 0)
    target_bid = bid_amt if bid_amt >= 70 else 70

    success_cnt, fail_cnt = 0, 0
    err_details = []

    for adg_id in adgroup_ids:
        r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": adg_id})
        if r_kw.status_code == 200:
            kws = r_kw.json()
            if not kws: continue

            update_payload = []
            for kw in kws:
                item = copy.deepcopy(kw)
                item["useGroupBidAmt"] = use_group_bid
                item["bidAmt"] = target_bid
                for k in ['regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']: 
                    item.pop(k, None)
                update_payload.append(item)

            for i in range(0, len(update_payload), 100):
                batch = update_payload[i:i+100]
                r_put = _do_req("PUT", api_key, secret_key, cid, "/ncc/keywords", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=batch)
                if r_put.status_code in [200, 201]:
                    success_cnt += len(batch)
                else:
                    for item in batch:
                        r_single = _do_req("PUT", api_key, secret_key, cid, f"/ncc/keywords/{item['nccKeywordId']}", params={"fields": "bidAmt,useGroupBidAmt"}, json_body=item)
                        if r_single.status_code in [200, 201]: success_cnt += 1
                        else: 
                            fail_cnt += 1
                            if len(err_details) < 5: err_details.append(f"[{item.get('keyword', '알수없음')}] 실패: {r_single.text}")

    msg = f"키워드 입찰가 변경 완료!\n(성공: {success_cnt}개, 실패: {fail_cnt}개)"
    if err_details: msg += "\n\n[상세 에러 내역]\n" + "\n".join(err_details)
    return jsonify({"ok": True, "message": msg})

def _extract_adgroup(src, target_camp_id, cid, biz_channel_id):
    res = {
        "nccCampaignId": str(target_camp_id), "customerId": int(cid), "name": src.get("name"),
        "adgroupType": src.get("adgroupType"), "useDailyBudget": src.get("useDailyBudget", False),
        "dailyBudget": src.get("dailyBudget", 0), "bidAmt": src.get("bidAmt", 70)
    }
    adgroup_type = src.get("adgroupType", "")
    if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and adgroup_type == "WEB_SITE":
        res["pcChannelId"] = res["mobileChannelId"] = str(biz_channel_id)
    else:
        if src.get("pcChannelId"): res["pcChannelId"] = str(src.get("pcChannelId"))
        if src.get("mobileChannelId"): res["mobileChannelId"] = str(src.get("mobileChannelId"))
    for k in ["useStoreUrl", "nccProductGroupId", "contentsNetworkBidAmt", "keywordPlusFlag", "contractId"]:
        if k in src: res[k] = src[k]
    return res

def _copy_adgroup_children(api_key, secret_key, cid, old_adg_id, new_adg_id, biz_channel_id):
    errors = []
    
    # 1. 키워드
    r_kw = _do_req("GET", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": old_adg_id})
    if r_kw.status_code == 200:
        new_kws = []
        for kw in r_kw.json():
            item = copy.deepcopy(kw)
            for k in ['nccKeywordId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'managedKeyword', 'referenceKey']: item.pop(k, None)
            item.update({"nccAdgroupId": str(new_adg_id), "customerId": int(cid)})
            new_kws.append(item)
        if new_kws:
            for i in range(0, len(new_kws), 100):
                batch = new_kws[i:i+100]
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=batch)
                if res.status_code not in [200, 201]:
                    for item in batch:
                        r_single = _do_req("POST", api_key, secret_key, cid, "/ncc/keywords", params={"nccAdgroupId": new_adg_id}, json_body=item)
                        if r_single.status_code not in [200, 201]: errors.append(f"키워드 에러: {r_single.text}")

    # 2. 소재 
    r_ad = _do_req("GET", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": old_adg_id})
    if r_ad.status_code == 200:
        ads = r_ad.json()
        def _post_ad(ad):
            item = copy.deepcopy(ad)
            ad_type = item.get("type", "")
            ref_data = item.get("referenceData", {})

            if ad_type in ["SHOPPING_PRODUCT_AD", "CATALOG_PRODUCT_AD"]:
                item["ad"] = {} 
                ref_key = item.get("referenceKey") or ref_data.get("mallProductId") or ref_data.get("id")
                if ref_key: item["referenceKey"] = str(ref_key)
            else:
                item.pop('referenceKey', None) 

            for k in ['nccAdId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceData', 'nccQi', 'enable']:
                item.pop(k, None)
                
            item.update({"nccAdgroupId": str(new_adg_id), "customerId": int(cid)})
            if "userLock" not in item: item["userLock"] = False
            
            if ad_type in ["SHOPPING_PRODUCT_AD", "CATALOG_PRODUCT_AD"]:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id, "isList": "true"}, json_body=[item])
            else:
                res = _do_req("POST", api_key, secret_key, cid, "/ncc/ads", params={"nccAdgroupId": new_adg_id}, json_body=item)

            if res.status_code not in [200, 201]: return f"소재 에러: {res.text}"
            return None
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(_post_ad, ad) for ad in ads]
            for f in as_completed(futures):
                err_msg = f.result()
                if err_msg: errors.append(err_msg)

    # 3. 확장소재 
    r_ext = _do_req("GET", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": old_adg_id})
    if r_ext.status_code == 200:
        for ext in r_ext.json():
            item = copy.deepcopy(ext)
            for k in ['adExtensionId', 'regTm', 'editTm', 'status', 'statusReason', 'inspectStatus', 'delFlag', 'referenceKey']: item.pop(k, None)
            item.update({"ownerId": str(new_adg_id), "customerId": int(cid)})
            ext_type = item.get("type")
            if biz_channel_id and biz_channel_id not in ["keep", "undefined"] and ext_type in ["SUB_LINK", "THUMBNAIL", "IMAGE", "TEXT"]:
                item["pcChannelId"] = item["mobileChannelId"] = str(biz_channel_id)
            res = _do_req("POST", api_key, secret_key, cid, "/ncc/ad-extensions", params={"ownerId": new_adg_id}, json_body=item)
            if res.status_code not in [200, 201] and "4003" not in res.text:
                errors.append(f"확장소재 에러: {res.text}")

    # 4. 제외검색어 
    rk_list = []
    r_rk = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{old_adg_id}/restricted-keywords")
    if r_rk.status_code == 200 and r_rk.json():
        rk_list = r_rk.json()
    else:
        r_rk2 = _do_req("GET", api_key, secret_key, cid, "/ncc/restricted-keywords", params={"nccAdgroupId": old_adg_id})
        if r_rk2.status_code == 200 and r_rk2.json(): rk_list = r_rk2.json()

    if rk_list:
        clean_kws = []
        for rk in rk_list:
            if isinstance(rk, dict):
                kw = rk.get("keyword") or rk.get("restrictedKeyword")
                if kw: clean_kws.append(kw)
            elif isinstance(rk, str):
                clean_kws.append(rk)
        
        if clean_kws:
            r_rk_put = _do_req("PUT", api_key, secret_key, cid, f"/ncc/adgroups/{new_adg_id}/restricted-keywords", json_body=clean_kws)
            if r_rk_put.status_code not in [200, 201, 204]:
                post_payload = [{"nccAdgroupId": str(new_adg_id), "customerId": int(cid), "keyword": k} for k in clean_kws]
                _do_req("POST", api_key, secret_key, cid, "/ncc/restricted-keywords", json_body=post_payload)

    return list(set(errors))

@app.route("/copy_adgroups_to_target", methods=["POST"])
def copy_adgroups_to_target():
    d = request.json or {}
    api_key, secret_key, cid = d.get("api_key"), d.get("customer_id")
    src_ids, target_camp_id, suffix, biz_channel_id = d.get("source_ids", []), d.get("target_campaign_id"), d.get("suffix", "_복사본"), d.get("biz_channel_id")

    results, all_errors = {"success": 0, "fail": 0}, []
    for src_id in src_ids:
        r_get = _do_req("GET", api_key, secret_key, cid, f"/ncc/adgroups/{src_id}")
        if r_get.status_code != 200: results["fail"] += 1; continue
        
        new_adg = _extract_adgroup(r_get.json(), target_camp_id, cid, biz_channel_id)
        new_adg["name"] += suffix
        r_post = _do_req("POST", api_key, secret_key, cid, "/ncc/adgroups", json_body=new_adg)
        
        if r_post.status_code in [200, 201]:
            results["success"] += 1
            errs = _copy_adgroup_children(api_key, secret_key, cid, src_id, r_post.json().get("nccAdgroupId"), biz_channel_id)
            all_errors.extend([f"[{new_adg['name']}] {e}" for e in errs])
        else: 
            results["fail"] += 1
            all_errors.append(f"[{new_adg['name']}] 생성 실패: {r_post.text}")

    return jsonify({"ok": True, "message": f"복사 완료! (성공: {results['success']}, 실패: {results['fail']})\n" + "\n".join(all_errors[:10])})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
