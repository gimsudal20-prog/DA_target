# -*- coding: utf-8 -*-
from __future__ import annotations

from flask import Blueprint, jsonify, current_app


def _view(service, name):
    def handler():
        try:
            return service.call(name)
        except KeyError as exc:
            current_app.logger.exception("copy/delete route handler is not registered: %s", name)
            return jsonify({"ok": False, "error": str(exc), "route": name}), 500
        except Exception as exc:
            # 배포 환경(gunicorn/Render/Heroku 등)에서는 처리 중 예외가 HTML 500으로 떨어지면
            # 프론트에서 원인을 확인할 수 없다. 복사/삭제 라우트는 항상 JSON으로 실패 사유를 돌려준다.
            current_app.logger.exception("copy/delete route failed: %s", name)
            return jsonify({
                "ok": False,
                "error": "서버 내부 오류가 발생했습니다. 작업 이력 또는 서버 로그의 details를 확인해주세요.",
                "details": f"{type(exc).__name__}: {exc}",
                "route": name,
            }), 500
    handler.__name__ = f"copy_delete_{name}"
    return handler


def create_copy_delete_blueprint(service):
    bp = Blueprint("copy_delete_routes", __name__)
    for rule, name in [
        ("/copy_entities_to_adgroups", "copy_entities_to_adgroups"),
        ("/copy_campaigns", "copy_campaigns"),
        ("/copy_adgroups_to_target", "copy_adgroups_to_target"),
        ("/bulk_delete_by_parent", "bulk_delete_by_parent"),
        ("/bulk_delete", "bulk_delete"),
        ("/delete_selected", "delete_selected"),
    ]:
        bp.add_url_rule(rule, endpoint=name, view_func=_view(service, name), methods=["POST"])
    return bp
