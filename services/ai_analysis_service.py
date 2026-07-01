# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests


class AIAnalysisService:
    """Fast, tool-based ad operations analysis for the local dashboard.

    The LLM is intentionally optional. Numeric selection, scoring, and sorting
    stay in deterministic Python code; a model can only rewrite the already
    computed result when an API key is configured.
    """

    def __init__(
        self,
        *,
        performance_stats_func: Callable[..., Dict[str, Any]],
        age_stats_func: Callable[..., Dict[str, Any]],
        time_stats_func: Callable[..., Dict[str, Any]],
        keyword_lookup_func: Callable[..., Tuple[Dict[str, Any], int]],
        ad_lookup_func: Optional[Callable[..., Tuple[Dict[str, Any], int]]] = None,
        fetch_stats_rows_for_targets_func: Callable[..., Tuple[List[Dict[str, Any]], List[str]]],
        stat_row_id_func: Callable[[Dict[str, Any]], str],
        build_empty_metric_func: Callable[[], Dict[str, Any]],
        add_stat_to_metric_func: Callable[[Dict[str, Any], Dict[str, Any]], None],
        finalize_metric_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        performance_number_func: Callable[[Any], float],
        stat_fields: List[str],
        fast_workers: int = 8,
    ) -> None:
        self.performance_stats = performance_stats_func
        self.age_stats = age_stats_func
        self.time_stats = time_stats_func
        self.keyword_lookup = keyword_lookup_func
        self.ad_lookup = ad_lookup_func
        self.fetch_stats_rows_for_targets = fetch_stats_rows_for_targets_func
        self.stat_row_id = stat_row_id_func
        self.build_empty_metric = build_empty_metric_func
        self.add_stat_to_metric = add_stat_to_metric_func
        self.finalize_metric = finalize_metric_func
        self.num = performance_number_func
        self.stat_fields = list(stat_fields or [])
        self.fast_workers = max(1, int(fast_workers or 8))
        self._cache_lock = threading.RLock()
        self._cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._cache_ttl_seconds = float(os.getenv("DA_AI_ANALYSIS_CACHE_TTL", "300") or 300)
        self._cache_max_items = 40
        self._max_detail_targets = self._env_int("DA_AI_MAX_DETAIL_TARGETS", 1200)
        self._max_account_detail_targets = self._env_int("DA_AI_MAX_ACCOUNT_DETAIL_TARGETS", 1000)

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return max(1, int(os.getenv(name, str(default)) or default))
        except Exception:
            return max(1, int(default))

    def analyze(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        payload = payload or {}
        question = str(payload.get("question") or "").strip()
        api_key = str(payload.get("api_key") or "").strip()
        secret_key = str(payload.get("secret_key") or "").strip()
        cid = str(payload.get("customer_id") or "").strip()
        if not api_key or not secret_key or not cid:
            return {"ok": False, "error": "API 정보 및 광고주를 선택해주세요."}, 400
        if not question:
            return {"ok": False, "error": "질문을 입력해주세요."}, 400

        tool = self._select_tool(question)
        cache_key = self._cache_key(payload, tool)
        cached = self._cache_get(cache_key)
        if cached is not None:
            cached = dict(cached)
            cached["cached"] = True
            cached.setdefault("steps", []).insert(0, self._step("이전 분석 결과 재사용", "같은 조건의 결과를 캐시에서 가져왔습니다.", 0))
            return cached, 200

        started = time.perf_counter()
        steps: List[Dict[str, Any]] = []
        steps.append(self._step("질문 의도 파악", f"{self._tool_label(tool)} 분석으로 분류했습니다.", started))
        try:
            if tool in {"low_conversion_campaigns", "waste_campaigns", "action_summary"}:
                analysis = self._analyze_performance_rows(payload, question, "campaign", tool)
            elif tool == "low_conversion_adgroups":
                analysis = self._analyze_performance_rows(payload, question, "adgroup", tool)
            elif tool == "no_conversion_reasons":
                analysis = self._analyze_no_conversion_reasons(payload, question)
            elif tool in {"low_conversion_keywords", "high_cost_keywords", "waste_keywords", "keyword_volume_top", "keyword_cpc_top"}:
                analysis = self._analyze_keywords(payload, question, tool)
            elif tool == "cpc_rising_keywords":
                analysis = self._analyze_keyword_cpc_change(payload, question)
            elif tool == "keyword_volume_growth":
                analysis = self._analyze_keyword_volume_change(payload, question)
            elif tool == "shopping_ads":
                analysis = self._analyze_ads(payload, question)
            elif tool == "time_performance":
                analysis = self._analyze_time(payload, question)
            elif tool == "age_performance":
                analysis = self._analyze_age(payload, question)
            else:
                analysis = self._analyze_performance_rows(payload, question, "campaign", "low_conversion_campaigns")
        except Exception as exc:
            return {
                "ok": False,
                "error": "AI 분석 데이터 수집 실패",
                "details": str(exc),
                "steps": steps + [self._step("분석 중 오류", str(exc), started)],
            }, 400

        steps.extend(analysis.pop("steps", []))
        deterministic_answer = self._build_answer(tool, analysis, question)
        llm_used = False
        llm_note = "LLM 미사용"
        answer = deterministic_answer

        llm_started = time.perf_counter()
        llm_answer, llm_note = self._try_llm_summary(question, tool, analysis, deterministic_answer)
        if llm_answer:
            answer = llm_answer
            llm_used = True
        steps.append(self._step("답변 정리", llm_note, llm_started))

        response = {
            "ok": True,
            "cached": False,
            "tool": tool,
            "tool_label": self._tool_label(tool),
            "answer": answer,
            "fallback_answer": deterministic_answer,
            "llm_used": llm_used,
            "summary_cards": analysis.get("summary_cards") or [],
            "columns": analysis.get("columns") or [],
            "rows": analysis.get("rows") or [],
            "warnings": analysis.get("warnings") or [],
            "errors": analysis.get("errors") or [],
            "suggested_questions": self._suggested_questions(tool),
            "steps": steps,
            "elapsed_ms": int((time.perf_counter() - started) * 1000),
            "date_label": analysis.get("date_label") or "",
            "scope_label": analysis.get("scope_label") or "",
        }
        self._cache_set(cache_key, response)
        return response, 200

    def _select_tool(self, question: str) -> str:
        q = re.sub(r"\s+", "", question or "").lower()
        keyword_intent = "키워드" in q
        cpc_intent = any(token in q for token in ("cpc", "클릭당비용", "클릭비용", "클릭단가", "비싼", "비싸"))
        volume_intent = any(token in q for token in ("노출", "노출수", "클릭", "클릭수", "imp", "impression", "click"))
        change_intent = any(token in q for token in ("전주", "전주와", "지난주", "직전", "비교", "대비", "오른", "상승", "증가", "늘어난", "늘었"))
        no_conversion_intent = any(token in q for token in ("전환없", "전환이없", "전환없는", "전환안", "전환이안", "무전환", "전환낮", "전환수낮"))
        cost_intent = any(token in q for token in ("비용", "돈", "지출", "소진", "많이쓴", "많이쓰인", "비싸"))
        if keyword_intent and volume_intent and change_intent and not cpc_intent:
            return "keyword_volume_growth"
        if keyword_intent and cpc_intent and change_intent:
            return "cpc_rising_keywords"
        if keyword_intent and any(token in q for token in ("낭비", "비효율", "효율낮", "줄여", "중지", "꺼야")):
            return "waste_keywords"
        if keyword_intent and no_conversion_intent:
            return "low_conversion_keywords"
        if keyword_intent and (cost_intent or (any(token in q for token in ("가장많", "상위", "top", "최고")) and any(token in q for token in ("비용", "돈", "지출", "소진")))):
            return "high_cost_keywords"
        if keyword_intent and cpc_intent:
            return "keyword_cpc_top"
        if keyword_intent and volume_intent and not cpc_intent:
            return "keyword_volume_top"
        if keyword_intent:
            return "low_conversion_keywords"
        if any(token in q for token in ("소재", "상품", "adid", "광고소재")):
            return "shopping_ads" if "쇼핑" in q or "상품" in q else "shopping_ads"
        if any(token in q for token in ("전환없", "전환이없", "전환안", "전환이안", "왜전환", "이유", "원인")):
            return "no_conversion_reasons"
        if any(token in q for token in ("시간대", "시간별", "몇시", "오전", "오후")):
            return "time_performance"
        if any(token in q for token in ("연령", "나이", "성별", "남성", "여성", "40대", "30대")):
            return "age_performance"
        if any(token in q for token in ("광고그룹", "그룹")):
            return "low_conversion_adgroups"
        if any(token in q for token in ("낭비", "비효율", "비용", "돈", "꺼야", "중지", "줄여")):
            return "waste_campaigns"
        if any(token in q for token in ("오늘", "먼저", "우선", "뭐부터", "점검", "추천")):
            return "action_summary"
        return "low_conversion_campaigns"

    def _tool_label(self, tool: str) -> str:
        return {
            "low_conversion_campaigns": "전환 낮은 캠페인",
            "low_conversion_adgroups": "전환 낮은 광고그룹",
            "low_conversion_keywords": "전환 낮은 키워드",
            "high_cost_keywords": "비용 상위 키워드",
            "no_conversion_reasons": "전환 없음 원인",
            "cpc_rising_keywords": "CPC 상승 키워드",
            "keyword_cpc_top": "CPC 상위 키워드",
            "keyword_volume_growth": "노출/클릭 증가 키워드",
            "keyword_volume_top": "노출/클릭 상위 키워드",
            "shopping_ads": "쇼핑/소재 성과",
            "waste_campaigns": "비용 비효율 캠페인",
            "waste_keywords": "비용 비효율 키워드",
            "time_performance": "시간대 성과",
            "age_performance": "연령대 성과",
            "action_summary": "우선 점검 항목",
        }.get(tool, "성과 분석")

    def _base_scope_payload(self, payload: Dict[str, Any], *, level: str) -> Dict[str, Any]:
        scope = self._normalize_scope(payload.get("target_scope") or payload.get("scope"))
        campaign_ids = [str(x or "").strip() for x in (payload.get("campaign_ids") or []) if str(x or "").strip()]
        adgroup_ids = [str(x or "").strip() for x in (payload.get("adgroup_ids") or []) if str(x or "").strip()]
        return {
            "target_scope": scope,
            "scope": scope,
            "campaign_ids": campaign_ids,
            "adgroup_ids": adgroup_ids,
            "campaign_type": payload.get("campaign_type") or payload.get("type_filter") or "all",
            "type_filter": payload.get("campaign_type") or payload.get("type_filter") or "all",
            "date_preset": payload.get("date_preset") or "last7days",
            "since": payload.get("since") or "",
            "until": payload.get("until") or "",
            "exclude_today": True if payload.get("exclude_today") is None else bool(payload.get("exclude_today")),
            "result_level": level,
            "level": level,
            "skip_bid_snapshots": True,
            "skip_ad_counts": True,
            "include_daily_metrics": False,
            "include_demographics": False,
        }

    def _campaign_type_bucket(self, value: Any) -> str:
        text = str(value or "").strip().upper()
        if not text:
            return ""
        text_compact = re.sub(r"[\s_-]+", "", text)
        if "쇼핑" in text or "카탈로그" in text:
            return "SHOPPING"
        if "파워링크" in text or "웹사이트" in text:
            return "WEB_SITE"
        if "SHOPPING" in text or "CATALOG" in text or "PRODUCT" in text:
            return "SHOPPING"
        if "WEB_SITE" in text or "POWERLINK" in text or text_compact in {"WEBSITE", "POWERLINK"} or text in {"WEB", "SEARCH"}:
            return "WEB_SITE"
        return text

    def _row_campaign_type_bucket(self, row: Dict[str, Any] | None) -> str:
        row = row if isinstance(row, dict) else {}
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}

        campaign_values = [
            row.get("campaignType"),
            row.get("campaignTp"),
            row.get("campaign_type"),
            row.get("campaign_tp"),
            row.get("campaignTypeName"),
            row.get("campaignTpNm"),
            row.get("campaign_type_label"),
            row.get("campaignTypeLabel"),
            raw.get("campaignType"),
            raw.get("campaignTp"),
            raw.get("campaign_type"),
            raw.get("campaign_tp"),
            raw.get("campaignTypeName"),
            raw.get("campaignTpNm"),
        ]
        for value in campaign_values:
            bucket = self._campaign_type_bucket(value)
            if bucket in {"SHOPPING", "WEB_SITE"}:
                return bucket

        adgroup_values = [
            row.get("adgroupType"),
            row.get("adgroupTp"),
            row.get("adgroup_type"),
            row.get("ad_group_type"),
            row.get("adgroupTypeName"),
            row.get("adgroup_type_label"),
            raw.get("adgroupType"),
            raw.get("adgroupTp"),
            raw.get("adgroup_type"),
            raw.get("ad_group_type"),
            raw.get("adgroupTypeName"),
        ]
        for value in adgroup_values:
            bucket = self._campaign_type_bucket(value)
            if bucket in {"SHOPPING", "WEB_SITE"}:
                return bucket

        ad_values = [
            row.get("type"),
            row.get("adType"),
            row.get("ad_type"),
            row.get("ad_type_label"),
            raw.get("type"),
            raw.get("adType"),
            raw.get("ad_type"),
        ]
        for value in ad_values:
            bucket = self._campaign_type_bucket(value)
            if bucket in {"SHOPPING", "WEB_SITE"}:
                return bucket

        if str(row.get("productName") or raw.get("productName") or row.get("referenceKey") or raw.get("referenceKey") or "").strip():
            return "SHOPPING"

        hint_text = " ".join(
            str(value or "")
            for value in [
                row.get("campaignName"),
                row.get("adgroupName"),
                raw.get("campaignName"),
                raw.get("adgroupName"),
            ]
        )
        bucket = self._campaign_type_bucket(hint_text)
        if bucket in {"SHOPPING", "WEB_SITE"}:
            return bucket
        return ""

    def _source_rows_for_campaign_type(self, rows: List[Dict[str, Any]], campaign_type: Any) -> List[Dict[str, Any]]:
        filter_bucket = self._campaign_type_bucket(campaign_type)
        if not filter_bucket or filter_bucket == "ALL":
            return list(rows or [])
        return [row for row in (rows or []) if self._row_campaign_type_bucket(row) == filter_bucket]

    def _detail_target_limit(self, scope: Any) -> int:
        scope_norm = self._normalize_scope(scope)
        if scope_norm == "account":
            return self._max_account_detail_targets
        return self._max_detail_targets

    def _trim_detail_targets(self, targets: List[Dict[str, Any]], scope: Any, label: str) -> Tuple[List[Dict[str, Any]], str]:
        limit = self._detail_target_limit(scope)
        if len(targets or []) <= limit:
            return targets or [], ""
        return (targets or [])[:limit], (
            f"{label} 대상이 {len(targets):,}개라 빠른 응답을 위해 먼저 {limit:,}개만 분석했습니다. "
            "정확도를 높이려면 선택 캠페인/광고그룹 또는 더 짧은 기간으로 다시 조회하세요."
        )

    def _analyze_performance_rows(self, payload: Dict[str, Any], question: str, level: str, tool: str) -> Dict[str, Any]:
        started = time.perf_counter()
        perf_payload = self._base_scope_payload(payload, level=level)
        result = self.performance_stats(
            str(payload.get("api_key") or "").strip(),
            str(payload.get("secret_key") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            perf_payload,
        )
        rows = [self._normalize_perf_row(row, level, question) for row in (result.get("rows") or [])]
        rows = self._rank_rows(rows, tool, question)
        columns = self._standard_columns(level)
        summary = result.get("summary") or {}
        filtered_count = len(rows)
        total_count = len(result.get("rows") or [])
        return {
            "rows": rows[:12],
            "columns": columns,
            "summary_cards": [
                {"label": "분석 대상", "value": f"{total_count:,}개", "note": self._scope_label(perf_payload.get("target_scope"))},
                {"label": "문제 후보", "value": f"{filtered_count:,}개", "note": "전환/비용 기준"},
                {"label": "총 비용", "value": self._won(summary.get("salesAmt")), "note": result.get("date_label") or ""},
            ],
            "warnings": result.get("warnings") or [],
            "errors": result.get("errors") or [],
            "date_label": result.get("date_label") or "",
            "scope_label": self._scope_label(perf_payload.get("target_scope")),
            "steps": [self._step("성과 데이터 조회", f"{level} 단위 {total_count:,}개를 계산했습니다.", started)],
        }

    def _normalize_perf_row(self, row: Dict[str, Any], level: str, question: str) -> Dict[str, Any]:
        metrics = row.get("metrics") or {}
        imp = self.num(metrics.get("impCnt"))
        clicks = self.num(metrics.get("clkCnt"))
        cost = self.num(metrics.get("salesAmt"))
        conv_key = "purchaseCcnt" if "구매" in str(question or "") else "ccnt"
        conv = self.num(metrics.get(conv_key))
        revenue_key = "purchaseConvAmt" if conv_key == "purchaseCcnt" else "convAmt"
        revenue = self.num(metrics.get(revenue_key))
        cvr = (conv / clicks * 100) if clicks > 0 else 0
        roas, roas_available = self._compute_roas(revenue, cost, conv)
        ctr = self.num(metrics.get("ctr")) if self.num(metrics.get("ctr")) > 0 else ((clicks / imp * 100) if imp > 0 else 0)
        cpc = self.num(metrics.get("cpc")) if self.num(metrics.get("cpc")) > 0 else ((cost / clicks) if clicks > 0 else 0)
        target_name = str(row.get("name") or row.get("campaign_name") or row.get("adgroup_name") or row.get("id") or "-")
        item = {
            "name": target_name,
            "campaign": str(row.get("campaign_name") or target_name if level == "campaign" else row.get("campaign_name") or "-"),
            "adgroup": str(row.get("adgroup_name") or target_name if level == "adgroup" else row.get("adgroup_name") or ""),
            "type": str(row.get("campaign_type_label") or row.get("campaign_type") or ""),
            "impCnt": int(round(imp)),
            "clkCnt": int(round(clicks)),
            "ccnt": round(conv, 2),
            "salesAmt": round(cost, 2),
            "convAmt": round(revenue, 2),
            "cpc": round(cpc, 2),
            "cvr": round(cvr, 2),
            "roas": round(roas, 2) if roas_available else None,
            "roas_available": roas_available,
            "ctr": round(ctr, 2),
        }
        assessment = self._row_assessment(imp, clicks, conv, cost, cvr, roas, ctr, cpc, roas_available=roas_available)
        item.update({
            "judgement": assessment["label"],
            "judgement_tone": assessment["tone"],
            "is_problem": assessment["is_problem"],
            "reason": assessment["reason"],
        })
        item["score"] = self._risk_score(item)
        return item

    def _rank_rows(self, rows: List[Dict[str, Any]], tool: str, question: str) -> List[Dict[str, Any]]:
        if tool == "high_cost_keywords":
            candidates = [row for row in rows if self.num(row.get("salesAmt")) > 0]
            return sorted(candidates, key=lambda r: (-self.num(r.get("salesAmt")), -self.num(r.get("clkCnt")), self.num(r.get("ccnt")), str(r.get("name") or "")))
        if tool == "waste_keywords":
            candidates = [
                row for row in rows
                if self.num(row.get("salesAmt")) > 0
                and bool(row.get("is_problem"))
                and str(row.get("judgement_tone") or "") in {"danger", "warning"}
            ]
            return sorted(candidates, key=lambda r: (-self.num(r.get("salesAmt")), -self.num(r.get("score")), -self.num(r.get("clkCnt")), str(r.get("name") or "")))
        if tool == "keyword_volume_top":
            q = str(question or "")
            q_norm = q.lower()
            wants_imp = "노출" in q or "imp" in q_norm or "impression" in q_norm
            wants_click = "클릭" in q or "click" in q_norm
            candidates = [
                row for row in rows
                if (wants_imp and self.num(row.get("impCnt")) > 0)
                or (wants_click and self.num(row.get("clkCnt")) > 0)
                or (not wants_imp and not wants_click and (self.num(row.get("impCnt")) > 0 or self.num(row.get("clkCnt")) > 0))
            ]
            if wants_imp and not wants_click:
                return sorted(candidates, key=lambda r: (-self.num(r.get("impCnt")), -self.num(r.get("clkCnt")), str(r.get("name") or "")))
            return sorted(candidates, key=lambda r: (-self.num(r.get("clkCnt")), -self.num(r.get("impCnt")), str(r.get("name") or "")))
        if tool == "keyword_cpc_top":
            candidates = [row for row in rows if self.num(row.get("clkCnt")) > 0 and self.num(row.get("cpc")) > 0]
            return sorted(candidates, key=lambda r: (-self.num(r.get("cpc")), -self.num(r.get("salesAmt")), -self.num(r.get("clkCnt")), str(r.get("name") or "")))
        min_clicks = 10 if tool != "low_conversion_keywords" else 5
        if "클릭" in str(question or ""):
            min_clicks = max(1, min_clicks)
        candidates = []
        for row in rows:
            clicks = self.num(row.get("clkCnt"))
            conv = self.num(row.get("ccnt"))
            cost = self.num(row.get("salesAmt"))
            roas = self.num(row.get("roas"))
            cvr = self.num(row.get("cvr"))
            if tool in {"waste_campaigns", "action_summary"}:
                include = cost > 0 and bool(row.get("is_problem"))
            else:
                include = clicks >= min_clicks and bool(row.get("is_problem")) and not (conv > 0 and roas >= self._target_roas())
            if include:
                candidates.append(row)
        return sorted(candidates, key=lambda r: (-self.num(r.get("score")), -self.num(r.get("salesAmt")), -self.num(r.get("clkCnt")), str(r.get("name") or "")))

    def _target_roas(self) -> float:
        try:
            return max(1.0, float(os.getenv("DA_AI_TARGET_ROAS", "100") or 100))
        except Exception:
            return 100.0

    def _min_revenue_for_roas(self) -> float:
        try:
            return max(0.0, float(os.getenv("DA_AI_MIN_REVENUE_FOR_ROAS", "100") or 100))
        except Exception:
            return 100.0

    def _roas_revenue_is_valid(self, revenue: float, conv: float) -> bool:
        # Some conversion reports store a lead/event default value such as 1.
        # Treat that as "no revenue value" so the UI does not show fake 0.01% ROAS.
        return revenue > max(self._min_revenue_for_roas(), conv)

    def _compute_roas(self, revenue: float, cost: float, conv: float) -> Tuple[float, bool]:
        if cost <= 0 or conv <= 0 or not self._roas_revenue_is_valid(revenue, conv):
            return 0.0, False
        return revenue / cost * 100, True

    def _row_assessment(self, imp: float, clicks: float, conv: float, cost: float, cvr: float, roas: float, ctr: float, cpc: float, *, roas_available: bool = True) -> Dict[str, Any]:
        target_roas = self._target_roas()
        high_roas = max(500.0, target_roas * 3.0)
        if imp <= 0:
            return {
                "label": "노출부족",
                "tone": "muted",
                "is_problem": False,
                "reason": "노출이 거의 없어 예산, 입찰가, 상태, 타겟 설정부터 확인해야 합니다.",
            }
        if clicks <= 0:
            return {
                "label": "저반응",
                "tone": "warning",
                "is_problem": imp >= 100,
                "reason": "노출은 있지만 클릭이 없어 소재 문구, 키워드 의도, 순위/입찰가를 먼저 봐야 합니다.",
            }
        if conv > 0:
            if cost <= 0:
                return {
                    "label": "효율양호",
                    "tone": "good",
                    "is_problem": False,
                    "reason": "비용 없이 전환이 잡혀 효율 문제로 보기 어렵습니다. 추적값과 비용 집계를 함께 확인하세요.",
                }
            if not roas_available:
                return {
                    "label": "ROAS제외",
                    "tone": "muted",
                    "is_problem": False,
                    "reason": "전환은 있으나 전환매출 값이 없거나 이벤트 기본값으로 보여 ROAS 판단에서 제외합니다. CPA/CVR 기준으로 보세요.",
                }
            if roas >= high_roas:
                return {
                    "label": "확장검토",
                    "tone": "good",
                    "is_problem": False,
                    "reason": f"ROAS가 {roas:,.2f}%로 매우 높아 줄일 후보가 아니라 예산/입찰 확대를 검토할 항목입니다.",
                }
            if roas >= target_roas:
                return {
                    "label": "효율양호",
                    "tone": "good",
                    "is_problem": False,
                    "reason": f"ROAS가 목표 기준({target_roas:,.0f}%) 이상이라 효율 낮은 후보로 보지 않습니다.",
                }
            if roas > 0:
                return {
                    "label": "ROAS점검",
                    "tone": "warning",
                    "is_problem": True,
                    "reason": f"전환은 있으나 ROAS가 {roas:,.2f}%로 목표 기준({target_roas:,.0f}%)보다 낮습니다.",
                }
            return {
                "label": "매출확인",
                "tone": "warning",
                "is_problem": True,
                "reason": "전환은 있으나 전환매출/ROAS가 거의 없어 매출 집계와 전환 이벤트 값을 확인해야 합니다.",
            }
        if clicks >= 30:
            return {
                "label": "무전환점검",
                "tone": "danger",
                "is_problem": True,
                "reason": "클릭 표본은 있는데 전환이 없어 검색어 의도, 랜딩/상품 가격, 전환 추적 누락을 우선 점검해야 합니다.",
            }
        if cost > 0 and cpc >= 500:
            return {
                "label": "CPC점검",
                "tone": "warning",
                "is_problem": True,
                "reason": "전환 없이 CPC가 높아 입찰가와 매체/키워드 확장 범위를 줄일 후보입니다.",
            }
        if cost > 0:
            return {
                "label": "표본확인",
                "tone": "warning",
                "is_problem": clicks >= 3,
                "reason": "클릭은 있으나 전환이 없어 조금 더 표본을 보되 검색어-소재-랜딩 연결을 확인하세요.",
            }
        if ctr < 0.2 and imp >= 1000:
            return {
                "label": "저반응",
                "tone": "warning",
                "is_problem": True,
                "reason": "노출 대비 클릭 반응이 낮아 소재/검색어 매칭 점검이 필요합니다.",
            }
        return {
            "label": "표본부족",
            "tone": "muted",
            "is_problem": False,
            "reason": "아직 비용·클릭 표본이 작아 효율 낮음으로 단정하지 않습니다.",
        }

    def _risk_score(self, row: Dict[str, Any]) -> float:
        clicks = self.num(row.get("clkCnt"))
        cost = self.num(row.get("salesAmt"))
        conv = self.num(row.get("ccnt"))
        cvr = self.num(row.get("cvr"))
        roas = self.num(row.get("roas") if row.get("roas") is not None else 0)
        roas_available = bool(row.get("roas_available"))
        cpc = self.num(row.get("cpc"))
        if conv > 0 and not roas_available:
            return round(min(18.0, cost / max(1.0, conv) / 900), 2)
        if conv > 0 and roas >= max(500.0, self._target_roas() * 3.0):
            return round(min(12.0, cost / 3000), 2)
        score = min(45, clicks * 0.45) + min(45, cost / 1200) + min(18, cpc / 180)
        if conv <= 0:
            score += 45
        if cvr < 1:
            score += 20
        if cost > 0 and roas_available and roas < self._target_roas():
            score += 10
        if conv > 0 and roas_available and roas >= self._target_roas():
            score -= 45
        return round(score, 2)

    def _row_reason(self, imp: float, clicks: float, conv: float, cost: float, cvr: float, roas: float, ctr: float, cpc: float) -> str:
        return str(self._row_assessment(imp, clicks, conv, cost, cvr, roas, ctr, cpc, roas_available=roas > 0).get("reason") or "")

    def _join_limited(self, values: List[Any], limit: int = 2) -> str:
        cleaned = []
        seen = set()
        for value in values or []:
            text = str(value or "").strip()
            if not text or text == "-":
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(text)
        if not cleaned:
            return "-"
        head = cleaned[:limit]
        suffix = f" 외 {len(cleaned) - limit}곳" if len(cleaned) > limit else ""
        return ", ".join(head) + suffix

    def _aggregate_keyword_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        buckets: Dict[str, Dict[str, Any]] = {}
        for row in rows or []:
            name = str(row.get("name") or row.get("keyword") or "").strip()
            if not name:
                continue
            key = re.sub(r"\s+", " ", name).casefold()
            bucket = buckets.setdefault(key, {
                "name": name,
                "keyword": name,
                "campaigns": [],
                "adgroups": [],
                "types": [],
                "impCnt": 0.0,
                "clkCnt": 0.0,
                "ccnt": 0.0,
                "salesAmt": 0.0,
                "convAmt": 0.0,
                "sourceCount": 0,
            })
            bucket["sourceCount"] += 1
            bucket["campaigns"].append(row.get("campaign"))
            bucket["adgroups"].append(row.get("adgroup"))
            bucket["types"].append(row.get("type"))
            for metric in ("impCnt", "clkCnt", "ccnt", "salesAmt", "convAmt"):
                bucket[metric] += self.num(row.get(metric))

        aggregated: List[Dict[str, Any]] = []
        for bucket in buckets.values():
            imp = self.num(bucket.get("impCnt"))
            clicks = self.num(bucket.get("clkCnt"))
            conv = self.num(bucket.get("ccnt"))
            cost = self.num(bucket.get("salesAmt"))
            revenue = self.num(bucket.get("convAmt"))
            cpc = cost / clicks if clicks > 0 else 0
            cvr = conv / clicks * 100 if clicks > 0 else 0
            roas, roas_available = self._compute_roas(revenue, cost, conv)
            assessment = self._row_assessment(imp, clicks, conv, cost, cvr, roas, 0, cpc, roas_available=roas_available)
            item = {
                "name": bucket.get("name") or "-",
                "keyword": bucket.get("keyword") or bucket.get("name") or "-",
                "campaign": self._join_limited(bucket.get("campaigns") or []),
                "adgroup": self._join_limited(bucket.get("adgroups") or []),
                "type": self._join_limited(bucket.get("types") or []),
                "impCnt": int(round(imp)),
                "clkCnt": int(round(clicks)),
                "ccnt": round(conv, 2),
                "salesAmt": round(cost, 2),
                "convAmt": round(revenue, 2),
                "cpc": round(cpc, 2),
                "cvr": round(cvr, 2),
                "roas": round(roas, 2) if roas_available else None,
                "roas_available": roas_available,
                "sourceCount": int(bucket.get("sourceCount") or 0),
                "judgement": assessment["label"],
                "judgement_tone": assessment["tone"],
                "is_problem": assessment["is_problem"],
                "reason": assessment["reason"],
            }
            item["score"] = self._risk_score(item)
            aggregated.append(item)
        return aggregated

    def _analyze_keywords(self, payload: Dict[str, Any], question: str, tool: str = "low_conversion_keywords") -> Dict[str, Any]:
        started = time.perf_counter()
        scope_payload = self._base_scope_payload(payload, level="keyword")
        lookup_scope = {"selected_campaigns": "campaign", "selected_adgroups": "adgroup"}.get(scope_payload["target_scope"], "account")
        keyword_payload = {
            "api_key": payload.get("api_key"),
            "secret_key": payload.get("secret_key"),
            "customer_id": payload.get("customer_id"),
            "scope": lookup_scope,
            "campaign_ids": scope_payload.get("campaign_ids") or [],
            "adgroup_ids": scope_payload.get("adgroup_ids") or [],
        }
        keyword_result, keyword_status = self.keyword_lookup(keyword_payload)
        if keyword_status != 200:
            raise RuntimeError(str((keyword_result or {}).get("error") or "키워드 조회 실패"))
        keyword_rows = keyword_result.get("rows") or []
        active_rows = [row for row in keyword_rows if str(row.get("status") or "").upper() != "OFF"]
        source_rows = self._source_rows_for_campaign_type(active_rows or keyword_rows, scope_payload.get("campaign_type"))
        warnings = list(keyword_result.get("warnings") or [])
        targets = []
        keyword_by_id: Dict[str, Dict[str, Any]] = {}
        for row in source_rows:
            keyword_id = str(row.get("keywordId") or "").strip()
            if not keyword_id:
                continue
            keyword_by_id[keyword_id] = row
            targets.append({
                "id": keyword_id,
                "campaign_type": self._row_campaign_type_bucket(row) or row.get("campaignType") or "",
                "name": row.get("keyword") or keyword_id,
            })
        targets, trim_warning = self._trim_detail_targets(targets, scope_payload.get("target_scope"), "키워드")
        if trim_warning:
            warnings.append(trim_warning)
            allowed_ids = {str((target or {}).get("id") or "").strip() for target in targets}
            keyword_by_id = {key: value for key, value in keyword_by_id.items() if key in allowed_ids}
        since, until, date_label = self._date_range_from_payload(payload)
        stat_rows, stat_errors = self.fetch_stats_rows_for_targets(
            str(payload.get("api_key") or "").strip(),
            str(payload.get("secret_key") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            targets,
            self.stat_fields,
            since,
            until,
            "",
            "",
            "allDays",
            self.fast_workers,
            True,
        )
        metrics_by_id: Dict[str, Dict[str, Any]] = defaultdict(self.build_empty_metric)
        for stat_row in stat_rows:
            stat_id = self.stat_row_id(stat_row)
            if stat_id:
                self.add_stat_to_metric(metrics_by_id[stat_id], stat_row)
        rows: List[Dict[str, Any]] = []
        for keyword_id, row in keyword_by_id.items():
            metrics = self.finalize_metric(metrics_by_id.get(keyword_id, self.build_empty_metric()))
            normalized = self._normalize_perf_row({
                "id": keyword_id,
                "name": row.get("keyword") or keyword_id,
                "campaign_name": row.get("campaignName") or "",
                "adgroup_name": row.get("adgroupName") or "",
                "campaign_type": self._row_campaign_type_bucket(row) or row.get("campaignType") or "",
                "metrics": metrics,
            }, "keyword", question)
            normalized["keyword"] = normalized["name"]
            rows.append(normalized)
        aggregated_rows = self._aggregate_keyword_rows(rows)
        ranked = self._rank_rows(aggregated_rows, tool, question)
        problem_note = {
            "high_cost_keywords": "비용 내림차순",
            "waste_keywords": "비용+전환효율 기준",
            "keyword_volume_top": "노출/클릭 규모 기준",
            "keyword_cpc_top": "CPC 내림차순",
        }.get(tool, "클릭/전환 기준")
        return {
            "rows": ranked[:12],
            "columns": self._standard_columns("keyword"),
            "summary_cards": [
                {"label": "등록 항목", "value": f"{len(source_rows):,}개", "note": "ON 키워드 우선"},
                {"label": "키워드명", "value": f"{len(aggregated_rows):,}개", "note": "동일 키워드 합산"},
                {"label": "선별 기준", "value": f"{len(ranked):,}개", "note": problem_note},
                {"label": "통계 행", "value": f"{len(stat_rows):,}개", "note": date_label},
            ],
            "warnings": warnings,
            "errors": stat_errors[:10],
            "date_label": date_label,
            "scope_label": self._scope_label(scope_payload.get("target_scope")),
            "steps": [
                self._step("키워드 목록 조회", f"키워드 {len(source_rows):,}개를 확인했습니다.", started),
                self._step("키워드 통계 계산", f"통계 응답 {len(stat_rows):,}행을 매칭했습니다.", started),
            ],
        }

    def _keyword_metrics_snapshot(
        self,
        payload: Dict[str, Any],
        scope_payload: Dict[str, Any],
        since: str,
        until: str,
        source_rows_override: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], int, List[str], List[str]]:
        if source_rows_override is None:
            lookup_scope = {"selected_campaigns": "campaign", "selected_adgroups": "adgroup"}.get(scope_payload["target_scope"], "account")
            keyword_payload = {
                "api_key": payload.get("api_key"),
                "secret_key": payload.get("secret_key"),
                "customer_id": payload.get("customer_id"),
                "scope": lookup_scope,
                "campaign_ids": scope_payload.get("campaign_ids") or [],
                "adgroup_ids": scope_payload.get("adgroup_ids") or [],
            }
            keyword_result, keyword_status = self.keyword_lookup(keyword_payload)
            if keyword_status != 200:
                raise RuntimeError(str((keyword_result or {}).get("error") or "키워드 조회 실패"))
            keyword_rows = keyword_result.get("rows") or []
            active_rows = [row for row in keyword_rows if str(row.get("status") or "").upper() != "OFF"]
            source_rows = self._source_rows_for_campaign_type(active_rows or keyword_rows, scope_payload.get("campaign_type"))
            warnings = list(keyword_result.get("warnings") or [])
        else:
            source_rows = list(source_rows_override or [])
            warnings = []
        targets = []
        keyword_by_id: Dict[str, Dict[str, Any]] = {}
        for row in source_rows:
            keyword_id = str(row.get("keywordId") or "").strip()
            if not keyword_id:
                continue
            keyword_by_id[keyword_id] = row
            targets.append({
                "id": keyword_id,
                "campaign_type": self._row_campaign_type_bucket(row) or row.get("campaignType") or "",
                "name": row.get("keyword") or keyword_id,
            })
        targets, trim_warning = self._trim_detail_targets(targets, scope_payload.get("target_scope"), "키워드")
        if trim_warning:
            warnings.append(trim_warning)
            allowed_ids = {str((target or {}).get("id") or "").strip() for target in targets}
            keyword_by_id = {key: value for key, value in keyword_by_id.items() if key in allowed_ids}
            source_rows = [row for row in source_rows if str((row or {}).get("keywordId") or "").strip() in allowed_ids]
        stat_rows, stat_errors = self.fetch_stats_rows_for_targets(
            str(payload.get("api_key") or "").strip(),
            str(payload.get("secret_key") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            targets,
            self.stat_fields,
            since,
            until,
            "",
            "",
            "allDays",
            self.fast_workers,
            True,
        )
        metrics_by_id: Dict[str, Dict[str, Any]] = defaultdict(self.build_empty_metric)
        for stat_row in stat_rows:
            stat_id = self.stat_row_id(stat_row)
            if stat_id:
                self.add_stat_to_metric(metrics_by_id[stat_id], stat_row)
        finalized = {
            keyword_id: self.finalize_metric(metrics_by_id.get(keyword_id, self.build_empty_metric()))
            for keyword_id in keyword_by_id.keys()
        }
        return list(keyword_by_id.values()), finalized, len(stat_rows), warnings, stat_errors[:10]

    def _comparison_ranges(self, payload: Dict[str, Any]) -> Tuple[str, str, str, str, str]:
        from datetime import date, timedelta

        since, until, date_label = self._date_range_from_payload(payload)
        since_d = date.fromisoformat(since)
        until_d = date.fromisoformat(until)
        days = max(1, (until_d - since_d).days + 1)
        prev_until = since_d - timedelta(days=1)
        prev_since = prev_until - timedelta(days=days - 1)
        return since, until, prev_since.isoformat(), prev_until.isoformat(), date_label

    def _analyze_keyword_cpc_change(self, payload: Dict[str, Any], question: str) -> Dict[str, Any]:
        started = time.perf_counter()
        scope_payload = self._base_scope_payload(payload, level="keyword")
        since, until, prev_since, prev_until, date_label = self._comparison_ranges(payload)
        source_rows, current_metrics, current_stat_count, warnings, errors = self._keyword_metrics_snapshot(payload, scope_payload, since, until)
        _, previous_metrics, previous_stat_count, prev_warnings, prev_errors = self._keyword_metrics_snapshot(payload, scope_payload, prev_since, prev_until, source_rows)
        warnings.extend(prev_warnings)
        errors.extend(prev_errors)

        rows: List[Dict[str, Any]] = []
        skipped_no_previous = 0
        for row in source_rows:
            keyword_id = str(row.get("keywordId") or "").strip()
            if not keyword_id:
                continue
            current = current_metrics.get(keyword_id) or self.build_empty_metric()
            previous = previous_metrics.get(keyword_id) or self.build_empty_metric()
            current_clicks = self.num(current.get("clkCnt"))
            previous_clicks = self.num(previous.get("clkCnt"))
            current_cpc = self.num(current.get("cpc")) if self.num(current.get("cpc")) > 0 else (self.num(current.get("salesAmt")) / current_clicks if current_clicks > 0 else 0)
            previous_cpc = self.num(previous.get("cpc")) if self.num(previous.get("cpc")) > 0 else (self.num(previous.get("salesAmt")) / previous_clicks if previous_clicks > 0 else 0)
            if current_clicks <= 0:
                continue
            if previous_clicks <= 0 or previous_cpc <= 0:
                skipped_no_previous += 1
                continue
            delta = current_cpc - previous_cpc
            delta_pct = delta / previous_cpc * 100
            if delta <= 0 and delta_pct <= 0:
                continue
            conv = self.num(current.get("ccnt"))
            rows.append({
                "name": row.get("keyword") or keyword_id,
                "campaign": row.get("campaignName") or "-",
                "adgroup": row.get("adgroupName") or "-",
                "clkCnt": int(round(current_clicks)),
                "ccnt": round(conv, 2),
                "salesAmt": round(self.num(current.get("salesAmt")), 2),
                "cpc": round(current_cpc, 2),
                "prevCpc": round(previous_cpc, 2),
                "cpcDelta": round(delta, 2),
                "cpcDeltaPct": round(delta_pct, 1),
                "reason": "CPC가 올랐는데 전환이 없어 입찰가/검색어 의도 점검 우선" if conv <= 0 else "CPC 상승 대비 전환 효율 확인 필요",
                "score": round(max(0, delta) + max(0, delta_pct * 8) + self.num(current.get("salesAmt")) / 500, 2),
            })
        rows.sort(key=lambda item: (-self.num(item.get("score")), -self.num(item.get("cpcDelta")), -self.num(item.get("clkCnt"))))
        if skipped_no_previous:
            warnings.append(f"직전 기간 클릭/CPC가 없는 키워드 {skipped_no_previous:,}개는 CPC 상승 계산에서 제외했습니다.")
        return {
            "rows": rows[:12],
            "columns": self._cpc_change_columns(),
            "summary_cards": [
                {"label": "분석 키워드", "value": f"{len(source_rows):,}개", "note": "통계 조회 대상"},
                {"label": "CPC 상승 후보", "value": f"{len(rows):,}개", "note": f"{prev_since}~{prev_until} 대비"},
                {"label": "직전 데이터 없음", "value": f"{skipped_no_previous:,}개", "note": "상승 계산 제외"},
                {"label": "통계 행", "value": f"{current_stat_count + previous_stat_count:,}개", "note": date_label},
            ],
            "warnings": warnings[:20],
            "errors": errors[:10],
            "date_label": date_label,
            "scope_label": self._scope_label(scope_payload.get("target_scope")),
            "steps": [
                self._step("키워드 목록 조회", f"키워드 {len(source_rows):,}개를 확인했습니다.", started),
                self._step("현재 기간 CPC 계산", f"{since}~{until} 통계 {current_stat_count:,}행을 계산했습니다.", started),
                self._step("직전 기간 CPC 비교", f"{prev_since}~{prev_until} 통계 {previous_stat_count:,}행과 비교했습니다.", started),
            ],
        }

    def _analyze_keyword_volume_change(self, payload: Dict[str, Any], question: str) -> Dict[str, Any]:
        started = time.perf_counter()
        scope_payload = self._base_scope_payload(payload, level="keyword")
        since, until, prev_since, prev_until, date_label = self._comparison_ranges(payload)
        source_rows, current_metrics, current_stat_count, warnings, errors = self._keyword_metrics_snapshot(payload, scope_payload, since, until)
        _, previous_metrics, previous_stat_count, prev_warnings, prev_errors = self._keyword_metrics_snapshot(payload, scope_payload, prev_since, prev_until, source_rows)
        warnings.extend(prev_warnings)
        errors.extend(prev_errors)

        q = re.sub(r"\s+", "", question or "").lower()
        wants_imp = "노출" in q or "imp" in q or "impression" in q
        wants_click = "클릭" in q or "click" in q
        if not wants_imp and not wants_click:
            wants_imp = wants_click = True

        rows: List[Dict[str, Any]] = []
        for row in source_rows:
            keyword_id = str(row.get("keywordId") or "").strip()
            if not keyword_id:
                continue
            current = current_metrics.get(keyword_id) or self.build_empty_metric()
            previous = previous_metrics.get(keyword_id) or self.build_empty_metric()
            current_imp = self.num(current.get("impCnt"))
            previous_imp = self.num(previous.get("impCnt"))
            current_clicks = self.num(current.get("clkCnt"))
            previous_clicks = self.num(previous.get("clkCnt"))
            imp_delta = current_imp - previous_imp
            click_delta = current_clicks - previous_clicks
            if (wants_imp and imp_delta > 0) or (wants_click and click_delta > 0):
                conv = self.num(current.get("ccnt"))
                cost = self.num(current.get("salesAmt"))
                cpc = self.num(current.get("cpc")) if self.num(current.get("cpc")) > 0 else (cost / current_clicks if current_clicks > 0 else 0)
                imp_pct = (imp_delta / previous_imp * 100) if previous_imp > 0 else (100 if imp_delta > 0 else 0)
                click_pct = (click_delta / previous_clicks * 100) if previous_clicks > 0 else (100 if click_delta > 0 else 0)
                score = (
                    (max(0, imp_delta) if wants_imp else 0)
                    + (max(0, click_delta) * 80 if wants_click else 0)
                    + (cost / 200)
                )
                if conv <= 0 and current_clicks > 0:
                    reason = "유입은 늘었지만 전환이 없어 검색어 의도와 랜딩/입찰가 점검 우선"
                elif click_delta > 0 and imp_delta <= 0:
                    reason = "클릭이 늘어 CTR 개선 가능성이 있어 소재/순위 변화 확인"
                elif imp_delta > 0 and click_delta <= 0:
                    reason = "노출은 늘었지만 클릭 반응은 따라오지 않아 문구/순위 점검"
                else:
                    reason = "노출과 클릭이 같이 늘어 확장 원인과 전환 효율 확인"
                rows.append({
                    "name": row.get("keyword") or keyword_id,
                    "campaign": row.get("campaignName") or "-",
                    "adgroup": row.get("adgroupName") or "-",
                    "impCnt": int(round(current_imp)),
                    "prevImpCnt": int(round(previous_imp)),
                    "impDelta": int(round(imp_delta)),
                    "impDeltaPct": round(imp_pct, 1),
                    "clkCnt": int(round(current_clicks)),
                    "prevClkCnt": int(round(previous_clicks)),
                    "clkDelta": int(round(click_delta)),
                    "clkDeltaPct": round(click_pct, 1),
                    "ccnt": round(conv, 2),
                    "salesAmt": round(cost, 2),
                    "cpc": round(cpc, 2),
                    "reason": reason,
                    "score": round(score, 2),
                })
        rows.sort(key=lambda item: (-self.num(item.get("score")), -self.num(item.get("clkDelta")), -self.num(item.get("impDelta"))))
        return {
            "rows": rows[:12],
            "columns": self._volume_change_columns(),
            "summary_cards": [
                {"label": "분석 키워드", "value": f"{len(source_rows):,}개", "note": "통계 조회 대상"},
                {"label": "노출/클릭 증가", "value": f"{len(rows):,}개", "note": f"{prev_since}~{prev_until} 대비"},
                {"label": "현재 통계 행", "value": f"{current_stat_count:,}개", "note": date_label},
                {"label": "직전 통계 행", "value": f"{previous_stat_count:,}개", "note": "비교 기간"},
            ],
            "warnings": warnings[:20],
            "errors": errors[:10],
            "date_label": date_label,
            "scope_label": self._scope_label(scope_payload.get("target_scope")),
            "steps": [
                self._step("키워드 목록 조회", f"키워드 {len(source_rows):,}개를 확인했습니다.", started),
                self._step("현재 기간 노출/클릭 계산", f"{since}~{until} 통계 {current_stat_count:,}행을 계산했습니다.", started),
                self._step("직전 기간과 비교", f"{prev_since}~{prev_until} 통계 {previous_stat_count:,}행과 비교했습니다.", started),
            ],
        }

    def _analyze_no_conversion_reasons(self, payload: Dict[str, Any], question: str) -> Dict[str, Any]:
        analysis = self._analyze_performance_rows(payload, question, "adgroup", "no_conversion_reasons")
        rows = analysis.get("rows") or []
        analysis["summary_cards"] = [
            *analysis.get("summary_cards", []),
            {"label": "진단 기준", "value": "노출→클릭→전환", "note": "병목 단계 자동 분류"},
        ]
        analysis["rows"] = rows
        analysis.setdefault("steps", []).append(self._step("원인 후보 분류", "노출, 클릭, CPC, 전환율 기준으로 병목을 분류했습니다.", time.perf_counter()))
        return analysis

    def _analyze_ads(self, payload: Dict[str, Any], question: str) -> Dict[str, Any]:
        started = time.perf_counter()
        if not self.ad_lookup:
            raise RuntimeError("소재 조회 함수가 연결되어 있지 않습니다.")
        scope_payload = self._base_scope_payload(payload, level="ad")
        lookup_scope = {"selected_campaigns": "campaign", "selected_adgroups": "adgroup"}.get(scope_payload["target_scope"], "account")
        ad_payload = {
            "api_key": payload.get("api_key"),
            "secret_key": payload.get("secret_key"),
            "customer_id": payload.get("customer_id"),
            "scope": lookup_scope,
            "campaign_ids": scope_payload.get("campaign_ids") or [],
            "adgroup_ids": scope_payload.get("adgroup_ids") or [],
        }
        ad_result, ad_status = self.ad_lookup(ad_payload)
        if ad_status != 200:
            raise RuntimeError(str((ad_result or {}).get("error") or "소재 조회 실패"))
        source_rows = ad_result.get("rows") or []
        active_rows = [row for row in source_rows if str(row.get("status") or "").upper() != "OFF"]
        active_rows = self._source_rows_for_campaign_type(active_rows, scope_payload.get("campaign_type"))
        warnings = list(ad_result.get("warnings") or [])
        if "쇼핑" in str(question or ""):
            shopping_rows = [row for row in active_rows if self._row_campaign_type_bucket(row) == "SHOPPING"]
            active_rows = shopping_rows or active_rows
        since, until, date_label = self._date_range_from_payload(payload)
        targets = []
        ad_by_id: Dict[str, Dict[str, Any]] = {}
        for row in active_rows:
            ad_id = str(row.get("adId") or "").strip()
            if not ad_id:
                continue
            ad_by_id[ad_id] = row
            targets.append({
                "id": ad_id,
                "campaign_type": self._row_campaign_type_bucket(row) or row.get("campaignType") or "",
                "name": row.get("productName") or row.get("headline") or row.get("summary") or ad_id,
            })
        targets, trim_warning = self._trim_detail_targets(targets, scope_payload.get("target_scope"), "소재")
        if trim_warning:
            warnings.append(trim_warning)
            allowed_ids = {str((target or {}).get("id") or "").strip() for target in targets}
            ad_by_id = {key: value for key, value in ad_by_id.items() if key in allowed_ids}
        stat_rows, stat_errors = self.fetch_stats_rows_for_targets(
            str(payload.get("api_key") or "").strip(),
            str(payload.get("secret_key") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            targets,
            self.stat_fields,
            since,
            until,
            "",
            "ad",
            "allDays",
            self.fast_workers,
            True,
        )
        metrics_by_id: Dict[str, Dict[str, Any]] = defaultdict(self.build_empty_metric)
        for stat_row in stat_rows:
            stat_id = self.stat_row_id(stat_row)
            if stat_id:
                self.add_stat_to_metric(metrics_by_id[stat_id], stat_row)
        rows = []
        for ad_id, row in ad_by_id.items():
            metrics = self.finalize_metric(metrics_by_id.get(ad_id, self.build_empty_metric()))
            label = row.get("productName") or row.get("headline") or row.get("summary") or ad_id
            normalized = self._normalize_perf_row({
                "id": ad_id,
                "name": label,
                "campaign_name": row.get("campaignName") or "",
                "adgroup_name": row.get("adgroupName") or "",
                "campaign_type": self._row_campaign_type_bucket(row) or row.get("campaignType") or "",
                "metrics": metrics,
            }, "ad", question)
            normalized["adId"] = ad_id
            normalized["status"] = row.get("status") or ""
            normalized["productName"] = row.get("productName") or ""
            normalized["headline"] = row.get("headline") or row.get("summary") or ""
            rows.append(normalized)
        ranked = self._rank_rows(rows, "low_conversion_keywords", question)
        return {
            "rows": ranked[:12],
            "columns": self._ad_columns(),
            "summary_cards": [
                {"label": "소재", "value": f"{len(active_rows):,}개", "note": "ON 소재 우선"},
                {"label": "문제 후보", "value": f"{len(ranked):,}개", "note": "클릭/전환 기준"},
                {"label": "통계 행", "value": f"{len(stat_rows):,}개", "note": date_label},
            ],
            "warnings": warnings,
            "errors": stat_errors[:10],
            "date_label": date_label,
            "scope_label": self._scope_label(scope_payload.get("target_scope")),
            "steps": [
                self._step("소재 목록 조회", f"소재 {len(active_rows):,}개를 확인했습니다.", started),
                self._step("소재별 통계 계산", f"소재 통계 {len(stat_rows):,}행을 매칭했습니다.", started),
            ],
        }

    def _analyze_time(self, payload: Dict[str, Any], question: str) -> Dict[str, Any]:
        started = time.perf_counter()
        result = self.time_stats(
            str(payload.get("api_key") or "").strip(),
            str(payload.get("secret_key") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            self._base_scope_payload(payload, level="adgroup"),
        )
        rows = []
        for row in result.get("rows") or []:
            metrics = row.get("metrics") or {}
            clicks = self.num(metrics.get("clkCnt"))
            conv = self.num(metrics.get("ccnt"))
            cost = self.num(metrics.get("salesAmt"))
            cvr = conv / clicks * 100 if clicks > 0 else 0
            rows.append({
                "name": row.get("label") or row.get("key") or "-",
                "impCnt": int(self.num(metrics.get("impCnt"))),
                "clkCnt": int(clicks),
                "ccnt": round(conv, 2),
                "salesAmt": round(cost, 2),
                "cvr": round(cvr, 2),
                "roas": round(self.num(metrics.get("ror")), 2),
                "reason": "전환 효율 점검 필요" if clicks > 0 and conv <= 0 else "시간대 성과 확인",
            })
        ranked = sorted(rows, key=lambda r: (self.num(r.get("ccnt")), -self.num(r.get("clkCnt")), -self.num(r.get("salesAmt"))))
        return {
            "rows": ranked[:12],
            "columns": self._time_age_columns("시간대"),
            "summary_cards": [
                {"label": "시간대", "value": f"{len(rows):,}개", "note": result.get("date_label") or ""},
                {"label": "총 비용", "value": self._won((result.get("summary") or {}).get("salesAmt")), "note": "기간 합계"},
                {"label": "총 전환", "value": self._fmt((result.get("summary") or {}).get("ccnt")), "note": "보고서 전환"},
            ],
            "warnings": result.get("warnings") or [],
            "errors": result.get("errors") or [],
            "date_label": result.get("date_label") or "",
            "scope_label": self._scope_label((self._base_scope_payload(payload, level="adgroup")).get("target_scope")),
            "steps": [self._step("시간대 데이터 조회", f"{len(rows):,}개 시간 구간을 계산했습니다.", started)],
        }

    def _analyze_age(self, payload: Dict[str, Any], question: str) -> Dict[str, Any]:
        started = time.perf_counter()
        result = self.age_stats(
            str(payload.get("api_key") or "").strip(),
            str(payload.get("secret_key") or "").strip(),
            str(payload.get("customer_id") or "").strip(),
            self._base_scope_payload(payload, level="adgroup"),
        )
        demographics = result.get("demographics") or {}
        raw_rows = demographics.get("age_rows") or demographics.get("age_gender_rows") or []
        rows = []
        for row in raw_rows:
            metrics = row.get("metrics") or row.get("total") or {}
            clicks = self.num(metrics.get("clkCnt"))
            conv = self.num(metrics.get("ccnt"))
            cost = self.num(metrics.get("salesAmt"))
            cvr = conv / clicks * 100 if clicks > 0 else 0
            rows.append({
                "name": row.get("label") or row.get("age_label") or row.get("age_key") or "-",
                "impCnt": int(self.num(metrics.get("impCnt"))),
                "clkCnt": int(clicks),
                "ccnt": round(conv, 2),
                "salesAmt": round(cost, 2),
                "cvr": round(cvr, 2),
                "roas": round(self.num(metrics.get("ror")), 2),
                "reason": "전환 효율 점검 필요" if clicks > 0 and conv <= 0 else "연령대 성과 확인",
            })
        ranked = sorted(rows, key=lambda r: (self.num(r.get("ccnt")), -self.num(r.get("clkCnt")), -self.num(r.get("salesAmt"))))
        return {
            "rows": ranked[:12],
            "columns": self._time_age_columns("연령대"),
            "summary_cards": [
                {"label": "연령 구간", "value": f"{len(rows):,}개", "note": result.get("date_label") or ""},
                {"label": "총 비용", "value": self._won((result.get("summary") or {}).get("salesAmt")), "note": "기간 합계"},
                {"label": "총 전환", "value": self._fmt((result.get("summary") or {}).get("ccnt")), "note": "보고서 전환"},
            ],
            "warnings": result.get("warnings") or [],
            "errors": result.get("errors") or [],
            "date_label": result.get("date_label") or "",
            "scope_label": self._scope_label((self._base_scope_payload(payload, level="adgroup")).get("target_scope")),
            "steps": [self._step("연령대 데이터 조회", f"{len(rows):,}개 연령 구간을 계산했습니다.", started)],
        }

    def _row_location(self, row: Dict[str, Any]) -> str:
        campaign = str(row.get("campaign") or "").strip()
        adgroup = str(row.get("adgroup") or "").strip()
        if campaign and campaign != "-" and adgroup and adgroup != "-":
            return f" · 위치 {campaign} > {adgroup}"
        if campaign and campaign != "-":
            return f" · 캠페인 {campaign}"
        if adgroup and adgroup != "-":
            return f" · 광고그룹 {adgroup}"
        return ""

    def _high_cost_reason(self, row: Dict[str, Any]) -> str:
        clicks = self.num(row.get("clkCnt"))
        conv = self.num(row.get("ccnt"))
        roas = self.num(row.get("roas"))
        cost = self.num(row.get("salesAmt"))
        judgement = str(row.get("judgement") or "").strip()
        tone = str(row.get("judgement_tone") or "").strip()
        roas_available = bool(row.get("roas_available"))
        if cost <= 0:
            return "비용이 없어 우선순위는 낮습니다."
        if conv > 0 and not roas_available:
            return "전환은 있으나 전환매출 값이 없어 ROAS 판단에서 제외합니다. 이 경우는 ROAS가 낮은 게 아니라 CPA/CVR 기준으로 봐야 합니다."
        if tone == "good":
            return f"비용 상위지만 판단은 '{judgement or '효율양호'}'입니다. 줄일 후보가 아니라 목표 ROAS/CPA 기준으로 유지 또는 확장 여부를 보세요."
        if clicks > 0 and conv <= 0:
            return "비용과 클릭은 있는데 전환이 없어 검색어 의도와 랜딩/전환 추적 점검이 우선입니다."
        if conv > 0 and roas >= 100:
            return "비용 상위지만 전환 성과가 있어 목표 ROAS/CPA 기준으로 유지 또는 확장 여부를 판단할 항목입니다."
        if conv > 0 and roas > 0:
            return "전환은 있으나 비용 대비 ROAS가 낮아 입찰가와 예산 배분을 점검할 항목입니다."
        return "비용 규모가 커 예산 점유율과 전환 기여도를 같이 봐야 합니다."

    def _roas_text(self, row: Dict[str, Any]) -> str:
        if row.get("roas_available") is False:
            return "-"
        return self._pct(row.get("roas"))

    def _keyword_line(self, idx: int, row: Dict[str, Any], *, include_location: bool = True, reason: str = "") -> str:
        location = self._row_location(row) if include_location else ""
        return (
            f"{idx}. {row.get('name') or '-'} - 비용 {self._won(row.get('salesAmt'))}, "
            f"클릭 {self._fmt(row.get('clkCnt'))}, CPC {self._won(row.get('cpc'))}, "
            f"전환 {self._fmt(row.get('ccnt'))}, CVR {self._pct(row.get('cvr'))}, "
            f"ROAS {self._roas_text(row)}, 판단 {row.get('judgement') or '-'}{location}. "
            f"{reason or row.get('reason') or ''}"
        )

    def _build_answer(self, tool: str, analysis: Dict[str, Any], question: str) -> str:
        rows = analysis.get("rows") or []
        label = self._tool_label(tool)
        if not rows:
            return f"{label} 기준으로 뚜렷한 문제 후보는 아직 보이지 않습니다.\n기간을 최근 30일로 넓히거나 캠페인/광고그룹 범위를 넓히면 더 안정적으로 판단할 수 있습니다."
        top = rows[:3]
        if tool == "cpc_rising_keywords":
            lines = [f"CPC가 오른 키워드는 {len(rows)}개입니다. 증가액과 전환 여부를 같이 보면 아래 순서가 우선입니다."]
            for idx, row in enumerate(top, start=1):
                lines.append(
                    f"{idx}. {row.get('name') or '-'} - 최근 CPC {self._won(row.get('cpc'))}, "
                    f"직전 {self._won(row.get('prevCpc'))}, 증가 {self._won(row.get('cpcDelta'))}({self._pct(row.get('cpcDeltaPct'))}), "
                    f"전환 {self._fmt(row.get('ccnt'))}{self._row_location(row)}. {row.get('reason') or ''}"
                )
            lines.append("CPC 상승 키워드는 입찰가, 확장검색어, 매체 지면, 전환 없는 클릭 유입 순서로 확인하세요.")
            return "\n".join(lines)
        if tool == "keyword_volume_growth":
            lines = [f"전 기간 대비 노출 또는 클릭이 증가한 키워드는 {len(rows)}개입니다. 증가폭과 전환 여부를 같이 보면 아래 순서가 우선입니다."]
            for idx, row in enumerate(top, start=1):
                lines.append(
                    f"{idx}. {row.get('name') or '-'} - 노출 {self._fmt(row.get('prevImpCnt'))}→{self._fmt(row.get('impCnt'))} "
                    f"(+{self._fmt(row.get('impDelta'))}), 클릭 {self._fmt(row.get('prevClkCnt'))}→{self._fmt(row.get('clkCnt'))} "
                    f"(+{self._fmt(row.get('clkDelta'))}), 전환 {self._fmt(row.get('ccnt'))}{self._row_location(row)}. {row.get('reason') or ''}"
                )
            lines.append("증가 키워드는 확장검색어/순위 변화로 유입이 늘어난 것인지, 전환 없는 클릭이 늘어난 것인지 먼저 나눠서 보세요.")
            return "\n".join(lines)
        if tool == "keyword_volume_top":
            lines = [f"{analysis.get('date_label') or '조회 기간'} 기준 노출/클릭 규모가 큰 키워드는 {len(rows)}개입니다."]
            for idx, row in enumerate(rows[:5], start=1):
                lines.append(
                    f"{idx}. {row.get('name') or '-'} - 노출 {self._fmt(row.get('impCnt'))}, "
                    f"클릭 {self._fmt(row.get('clkCnt'))}, 전환 {self._fmt(row.get('ccnt'))}, "
                    f"CPC {self._won(row.get('cpc'))}, 비용 {self._won(row.get('salesAmt'))}{self._row_location(row)}. "
                    f"{row.get('reason') or ''}"
                )
            lines.append("볼륨 상위 키워드는 전환이 같이 따라오는지, 클릭만 늘고 전환이 없는지로 운영 액션을 나누면 됩니다.")
            return "\n".join(lines)
        if tool == "keyword_cpc_top":
            lines = [f"{analysis.get('date_label') or '조회 기간'} 기준 CPC가 높은 키워드는 {len(rows)}개입니다."]
            for idx, row in enumerate(rows[:5], start=1):
                lines.append(
                    f"{idx}. {row.get('name') or '-'} - CPC {self._won(row.get('cpc'))}, "
                    f"클릭 {self._fmt(row.get('clkCnt'))}, 전환 {self._fmt(row.get('ccnt'))}, "
                    f"비용 {self._won(row.get('salesAmt'))}{self._row_location(row)}. {row.get('reason') or ''}"
                )
            lines.append("CPC 상위 키워드는 입찰가가 높아서인지, 순위/매체/확장검색어 때문에 비싼 클릭이 들어오는지 먼저 확인하세요.")
            return "\n".join(lines)
        if tool == "high_cost_keywords":
            leader = rows[0]
            lines = [
                f"{analysis.get('date_label') or '조회 기간'} 기준 비용이 가장 많이 쓰인 키워드는 '{leader.get('name') or '-'}'입니다.",
                "아래 순서는 전환 낮음 기준이 아니라 비용 내림차순입니다."
            ]
            for idx, row in enumerate(rows[:5], start=1):
                lines.append(self._keyword_line(idx, row, reason=self._high_cost_reason(row)))
            zero_conv = [row for row in rows[:5] if self.num(row.get("clkCnt")) > 0 and self.num(row.get("ccnt")) <= 0]
            if zero_conv:
                lines.append(f"먼저 '{zero_conv[0].get('name') or '-'}'처럼 비용과 클릭은 있는데 전환이 없는 키워드는 검색어 의도, 실제 유입 검색어, 랜딩/상품 상태, 전환 추적을 같이 확인하세요.")
            else:
                lines.append("비용 상위 키워드 중 전환이 있는 항목은 바로 줄이기보다 ROAS와 목표 CPA 기준으로 증액/유지 여부를 판단하세요.")
            return "\n".join(lines)
        if tool == "waste_keywords":
            lines = [f"비용을 쓰지만 효율이 낮은 키워드는 {len(rows)}개입니다. 비용 규모가 큰 순서로 먼저 봐야 합니다."]
            for idx, row in enumerate(rows[:5], start=1):
                lines.append(self._keyword_line(idx, row))
            lines.append("우선 전환 0건이면서 비용이 큰 키워드는 입찰가를 낮추고, 검색어 보고서에서 무관 유입은 제외 키워드로 빼는 것이 좋습니다.")
            return "\n".join(lines)
        if tool == "shopping_ads":
            lines = [f"소재/상품 기준 문제 후보는 {len(rows)}개입니다. 클릭은 있는데 전환이 없는 소재부터 보는 게 좋습니다."]
            for idx, row in enumerate(top, start=1):
                lines.append(
                    f"{idx}. {row.get('name') or '-'} - 클릭 {self._fmt(row.get('clkCnt'))}, "
                    f"전환 {self._fmt(row.get('ccnt'))}, CPC {self._won(row.get('cpc'))}, 비용 {self._won(row.get('salesAmt'))}. "
                    f"{row.get('reason') or ''}"
                )
            lines.append("쇼핑 소재는 상품명/이미지/가격/랜딩 재고와 소재 입찰가를 같이 확인하세요.")
            return "\n".join(lines)
        if tool == "no_conversion_reasons":
            lines = [f"전환이 없는 이유는 한 가지로 단정하기보다 병목 단계별로 봐야 합니다. 지금은 아래 항목이 우선입니다."]
        else:
            lines = [f"{label} 후보는 {len(rows)}개입니다. 우선순위는 아래 순서로 보는 게 좋습니다."]
        for idx, row in enumerate(top, start=1):
            if tool == "low_conversion_keywords":
                lines.append(self._keyword_line(idx, row))
            else:
                lines.append(
                    f"{idx}. {row.get('name') or '-'} - 클릭 {self._fmt(row.get('clkCnt'))}, "
                    f"전환 {self._fmt(row.get('ccnt'))}, 비용 {self._won(row.get('salesAmt'))}, "
                    f"CPC {self._won(row.get('cpc'))}, CVR {self._pct(row.get('cvr'))}. {row.get('reason') or ''}"
                )
        lines.append("먼저 비용과 클릭이 있는데 전환이 없는 항목부터 검색어 의도, 소재 메시지, 랜딩/상품 상태, 전환 추적을 확인하세요.")
        return "\n".join(lines)

    def _try_llm_summary(self, question: str, tool: str, analysis: Dict[str, Any], fallback: str) -> Tuple[str, str]:
        key = os.getenv("DA_AI_OPENAI_API_KEY") or os.getenv("OPENAI_API_KEY") or ""
        if not key:
            return "", "빠른 계산 답변을 표시했습니다."
        if str(os.getenv("DA_AI_DISABLE_LLM") or "").strip().lower() in {"1", "true", "yes", "y"}:
            return "", "빠른 계산 답변을 표시했습니다."
        model = os.getenv("DA_AI_MODEL") or "gpt-4o-mini"
        timeout = float(os.getenv("DA_AI_LLM_TIMEOUT", "8") or 8)
        packet = {
            "question": question,
            "tool_key": tool,
            "tool": self._tool_label(tool),
            "date_label": analysis.get("date_label"),
            "scope_label": analysis.get("scope_label"),
            "summary_cards": analysis.get("summary_cards"),
            "rows": (analysis.get("rows") or [])[:8],
            "fallback_answer": fallback,
        }
        prompt = (
            "너는 네이버 검색광고 운영 분석 비서다. 숫자는 제공된 JSON만 근거로 사용한다. "
            "질문 의도를 절대 바꾸지 않는다. 특히 비용/가장 많이 쓴 질문은 비용순이라고 답하고 전환 낮은 후보라고 바꾸지 않는다. "
            "rows의 judgement_tone이 good이면 절대 효율이 낮다거나 줄일 후보라고 말하지 말고 유지/확장 검토로 말한다. "
            "rows의 roas_available이 false이면 ROAS가 낮다고 말하지 말고 전환매출 값이 없어 ROAS 판단에서 제외한다고 말한다. "
            "동일 키워드는 합산된 값일 수 있으니 등록 위치가 있으면 함께 언급한다. "
            "추측하지 말고 한국어로 짧게 답한다. 첫 문장에 결론을 말하고, 근거 2~5개와 다음 액션을 제안한다."
        )
        try:
            res = requests.post(
                "https://api.openai.com/v1/responses",
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json={
                    "model": model,
                    "input": [
                        {"role": "system", "content": [{"type": "input_text", "text": prompt}]},
                        {"role": "user", "content": [{"type": "input_text", "text": json.dumps(packet, ensure_ascii=False)}]},
                    ],
                    "max_output_tokens": 420,
                    "temperature": 0.2,
                },
                timeout=timeout,
            )
            if res.status_code != 200:
                return "", f"요약 모델 응답 지연으로 계산 답변을 표시했습니다. 상태 {res.status_code}"
            data = res.json()
            text = str(data.get("output_text") or "").strip()
            if not text:
                text = self._extract_response_text(data)
            if text:
                return text, f"요약 모델로 계산 결과를 정리했습니다. 모델: {model}"
            return "", "요약 모델 응답이 비어 있어 계산 답변을 표시했습니다."
        except Exception as exc:
            return "", f"요약 모델 호출 지연으로 계산 답변을 표시했습니다. {exc}"

    def _extract_response_text(self, data: Dict[str, Any]) -> str:
        texts: List[str] = []
        for item in data.get("output") or []:
            for content in (item or {}).get("content") or []:
                text = content.get("text") if isinstance(content, dict) else ""
                if text:
                    texts.append(str(text))
        return "\n".join(texts).strip()

    def _standard_columns(self, level: str) -> List[Dict[str, str]]:
        first = "키워드" if level == "keyword" else ("광고그룹" if level == "adgroup" else "캠페인")
        cols = [{"key": "name", "label": first}]
        if level in {"keyword", "adgroup"}:
            cols.append({"key": "campaign", "label": "캠페인"})
        if level == "keyword":
            cols.append({"key": "adgroup", "label": "광고그룹"})
            cols.append({"key": "sourceCount", "label": "등록수"})
        cols.extend([
            {"key": "impCnt", "label": "노출"},
            {"key": "clkCnt", "label": "클릭"},
            {"key": "ccnt", "label": "전환"},
            {"key": "salesAmt", "label": "비용"},
            {"key": "cpc", "label": "CPC"},
            {"key": "cvr", "label": "CVR"},
            {"key": "roas", "label": "ROAS"},
            {"key": "judgement", "label": "판단"},
            {"key": "reason", "label": "근거"},
        ])
        return cols

    def _cpc_change_columns(self) -> List[Dict[str, str]]:
        return [
            {"key": "name", "label": "키워드"},
            {"key": "campaign", "label": "캠페인"},
            {"key": "adgroup", "label": "광고그룹"},
            {"key": "clkCnt", "label": "클릭"},
            {"key": "ccnt", "label": "전환"},
            {"key": "cpc", "label": "최근 CPC"},
            {"key": "prevCpc", "label": "직전 CPC"},
            {"key": "cpcDelta", "label": "증가액"},
            {"key": "cpcDeltaPct", "label": "증가율"},
            {"key": "reason", "label": "판단"},
        ]

    def _volume_change_columns(self) -> List[Dict[str, str]]:
        return [
            {"key": "name", "label": "키워드"},
            {"key": "campaign", "label": "캠페인"},
            {"key": "adgroup", "label": "광고그룹"},
            {"key": "impCnt", "label": "최근 노출"},
            {"key": "prevImpCnt", "label": "직전 노출"},
            {"key": "impDelta", "label": "노출 증가"},
            {"key": "impDeltaPct", "label": "노출 증가율"},
            {"key": "clkCnt", "label": "최근 클릭"},
            {"key": "prevClkCnt", "label": "직전 클릭"},
            {"key": "clkDelta", "label": "클릭 증가"},
            {"key": "clkDeltaPct", "label": "클릭 증가율"},
            {"key": "ccnt", "label": "전환"},
            {"key": "reason", "label": "판단"},
        ]

    def _ad_columns(self) -> List[Dict[str, str]]:
        return [
            {"key": "name", "label": "소재/상품"},
            {"key": "campaign", "label": "캠페인"},
            {"key": "adgroup", "label": "광고그룹"},
            {"key": "impCnt", "label": "노출"},
            {"key": "clkCnt", "label": "클릭"},
            {"key": "ccnt", "label": "전환"},
            {"key": "salesAmt", "label": "비용"},
            {"key": "cpc", "label": "CPC"},
            {"key": "cvr", "label": "CVR"},
            {"key": "reason", "label": "판단"},
        ]

    def _time_age_columns(self, first_label: str) -> List[Dict[str, str]]:
        return [
            {"key": "name", "label": first_label},
            {"key": "impCnt", "label": "노출"},
            {"key": "clkCnt", "label": "클릭"},
            {"key": "ccnt", "label": "전환"},
            {"key": "salesAmt", "label": "비용"},
            {"key": "cvr", "label": "CVR"},
            {"key": "reason", "label": "판단"},
        ]

    def _date_range_from_payload(self, payload: Dict[str, Any]) -> Tuple[str, str, str]:
        # Reuse performance_stats date parsing by issuing a tiny empty-ish campaign call is overkill;
        # keep only the supported presets used by the AI panel.
        from datetime import date, datetime, timedelta

        today = date.today()
        try:
            today = datetime.now().date()
        except Exception:
            pass
        yesterday = today - timedelta(days=1)
        preset = str(payload.get("date_preset") or "last7days").lower()
        if preset == "custom" and payload.get("since"):
            since = str(payload.get("since"))
            until = str(payload.get("until") or since)
            return since, until, "직접 기간"
        if preset in {"yesterday", "어제"}:
            return yesterday.isoformat(), yesterday.isoformat(), "어제"
        if preset in {"last30days", "last_30_days"}:
            return (yesterday - timedelta(days=29)).isoformat(), yesterday.isoformat(), "최근 30일"
        return (yesterday - timedelta(days=6)).isoformat(), yesterday.isoformat(), "최근 7일"

    def _normalize_scope(self, value: Any) -> str:
        raw = str(value or "account").strip().lower()
        if raw in {"campaign", "campaigns", "selected_campaign", "selected_campaigns"}:
            return "selected_campaigns"
        if raw in {"adgroup", "adgroups", "selected_adgroup", "selected_adgroups"}:
            return "selected_adgroups"
        return "account"

    def _scope_label(self, scope: Any) -> str:
        return {
            "selected_campaigns": "선택 캠페인",
            "selected_adgroups": "선택 광고그룹",
            "account": "계정 전체",
        }.get(str(scope or "account"), "계정 전체")

    def _suggested_questions(self, tool: str) -> List[str]:
        return [
            "전환이 없는 이유가 뭐야?",
            "최근 비용 가장 많이 쓰인 키워드가 뭐야?",
            "최근 CPC가 오른 키워드 알려줘",
            "전 주 대비 노출수나 클릭수가 상승한 키워드 알려줘",
            "클릭수가 높은 키워드 알려줘",
            "쇼핑 소재 중 전환 없는 상품이 뭐야?",
            "전환 수 낮은 캠페인이 뭐야?",
            "클릭은 많은데 전환 없는 키워드 알려줘",
            "비용 많이 쓰는데 효율 낮은 키워드 알려줘",
        ]

    def _step(self, title: str, detail: str, started: float) -> Dict[str, Any]:
        elapsed = int((time.perf_counter() - started) * 1000) if started else 0
        return {"title": title, "detail": detail, "elapsed_ms": elapsed}

    def _cache_key(self, payload: Dict[str, Any], tool: str) -> str:
        raw = {
            "version": "ai-judgement-v9-keyword-intents",
            "tool": tool,
            "question": str(payload.get("question") or "").strip().lower(),
            "api": self._hash(payload.get("api_key")),
            "secret": self._hash(payload.get("secret_key")),
            "customer_id": str(payload.get("customer_id") or "").strip(),
            "scope": self._normalize_scope(payload.get("target_scope") or payload.get("scope")),
            "campaign_type": str(payload.get("campaign_type") or payload.get("type_filter") or "all"),
            "date_preset": str(payload.get("date_preset") or "last7days"),
            "since": str(payload.get("since") or ""),
            "until": str(payload.get("until") or ""),
            "campaign_ids": sorted([str(x or "") for x in (payload.get("campaign_ids") or [])]),
            "adgroup_ids": sorted([str(x or "") for x in (payload.get("adgroup_ids") or [])]),
        }
        return hashlib.sha256(json.dumps(raw, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()

    def _cache_get(self, key: str) -> Optional[Dict[str, Any]]:
        now = time.time()
        with self._cache_lock:
            item = self._cache.get(key)
            if not item:
                return None
            ts, value = item
            if now - ts > self._cache_ttl_seconds:
                self._cache.pop(key, None)
                return None
            return json.loads(json.dumps(value, ensure_ascii=False))

    def _cache_set(self, key: str, value: Dict[str, Any]) -> None:
        now = time.time()
        with self._cache_lock:
            stale = [k for k, (ts, _) in self._cache.items() if now - ts > self._cache_ttl_seconds]
            for stale_key in stale:
                self._cache.pop(stale_key, None)
            while len(self._cache) >= self._cache_max_items:
                oldest = min(self._cache, key=lambda k: self._cache[k][0])
                self._cache.pop(oldest, None)
            self._cache[key] = (now, json.loads(json.dumps(value, ensure_ascii=False)))

    def _hash(self, value: Any) -> str:
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()

    def _fmt(self, value: Any) -> str:
        number = self.num(value)
        if abs(number - round(number)) < 0.001:
            return f"{int(round(number)):,}"
        return f"{number:,.2f}".rstrip("0").rstrip(".")

    def _won(self, value: Any) -> str:
        return f"{self._fmt(value)}원"

    def _pct(self, value: Any) -> str:
        return f"{self._fmt(value)}%"
