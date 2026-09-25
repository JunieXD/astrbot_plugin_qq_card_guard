import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest
from conftest import A, G, U

from qq_card_guard.config import GuardError, Policy, parse_settings
from qq_card_guard.rules import Member, judge, matches


@pytest.mark.parametrize(
    "card,valid",
    [
        ("大三-某某大学", True),
        (" 大三-某某大学 ", True),
        ("某某大学", False),
        ("", False),
        ("大三-某某大学\n附加说明", False),
        ("大三-a", False),
    ],
)
def test_full_card_match(card, valid):
    assert matches(Policy(G), card) is valid


def test_regex_timeout_and_length():
    with pytest.raises(GuardError, match="超时"):
        matches(Policy(G, pattern=r"(a|aa)+$"), "a" * 180 + "!")
    with pytest.raises(GuardError, match="长度"):
        matches(Policy(G), "x" * 513)


def test_case_insensitive_and_no_trim():
    assert matches(Policy(G, pattern="ABC", ignore_case=True), "abc")
    assert not matches(Policy(G, pattern="ABC", strip_spaces=False), " ABC")


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"role": "admin"}, "exempt"),
        ({"role": "owner"}, "exempt"),
        ({"uid": A}, "exempt"),
        ({"title": "优秀同学"}, "exempt"),
        ({"level": 10}, "exempt"),
        ({"role": ""}, "unknown"),
        ({"level": None}, "unknown"),
        ({"title": None}, "unknown"),
        ({"joined": None}, "unknown"),
        ({"level": 0}, "invalid"),
        ({"robot": True}, "exempt"),
    ],
)
def test_protective_unknowns(changes, expected):
    member = Member(U, "member", "bad", 1700000000, 0, 1, "", 0)
    assert judge(Policy(G, protect_level=10), replace(member, **changes), A).state == expected


def test_qq_level_zero_is_missing_but_group_zero_is_valid():
    member = Member.parse(
        dict(
            user_id=U,
            role="member",
            card="bad",
            join_time=1,
            level="0",
            qq_level=0,
            title="",
            shut_up_timestamp=0,
        )
    )
    assert member.level == 0 and member.qq_level is None
    assert judge(Policy(G, protect_qq_level=5), member, A).state == "unknown"


def test_config_isolation_and_duplicate_groups():
    result = parse_settings({"groups": [{"group_id": G, "pattern": "("}, {"group_id": "300002"}]})
    assert len(result.errors) == 1
    assert result.group("300002").mode == "仅观察"
    assert not parse_settings({"groups": [{"group_id": G}, {"group_id": G}]}).groups


@pytest.mark.parametrize(
    "text", ["{QQ号.__class__}", "{QQ号[0]}", "{QQ号!r}", "{QQ号:>10}", "{unknown}", "{"]
)
def test_templates_cannot_access_attributes(text):
    result = parse_settings({"groups": [{"group_id": G, "reminder": text}]})
    assert result.errors and not result.groups


def test_stages_and_custom_text():
    p = parse_settings(
        {
            "groups": [
                {
                    "group_id": G,
                    "stage_minutes": ["0", "5", "30"],
                    "stage_reminders": ["首次提醒", "第{违规轮次}轮：{禁言分钟}分钟"],
                }
            ]
        }
    ).group(G)
    assert p.render(U, 1, 0) == "首次提醒"
    assert p.render(U, 2, 5) == "第2轮：5分钟"
    assert p.stage(10).minutes == 30


def test_schema_defaults_are_valid_and_cover_settings():
    schema = json.loads(Path("_conf_schema.json").read_text(encoding="utf-8"))

    def defaults(items):
        return {
            k: defaults(v["items"]) if v["type"] == "object" else v.get("default") for k, v in items.items()
        }

    raw = defaults(schema)
    group = defaults(schema["groups"]["templates"]["group"]["items"])
    group["group_id"] = G
    raw["groups"] = [group]
    settings = parse_settings(raw)
    assert not settings.errors and not settings.enabled
    assert asdict(settings.group(G)) == asdict(Policy(G))
    assert set(schema["pace"]["items"]) == set(asdict(settings.pace))
    assert all(v["type"] != "template_list" for v in schema["groups"]["templates"]["group"]["items"].values())
