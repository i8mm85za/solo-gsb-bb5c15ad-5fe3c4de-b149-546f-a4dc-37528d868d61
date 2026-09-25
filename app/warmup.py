"""仪器预热稳定放行：在已提交读数中寻找最早完成的连续稳定段。

纯只读查询：仅读取 committed 表中已提交包的原始 payload，不触碰缓冲包、
作废序号、重复、告警与校准数据，也不改写任何已存内容。

候选读数：
- 仅取 metric 相同的 measurement；有效时间优先用 measurement 自身 ts，
  否则回退到所属包的 sent_at；两者都没有的测量直接忽略
  （计入 excluded_no_time_count）；
- 仅保留有效时间落在半开区间 [from, to) 内的测量；
- 单位必须与请求要求的 unit 逐字相同（读数缺 unit 视为不符，计入
  excluded_unit_mismatch_count，不参与判定）；
- 有效时间归一化为 UTC（naive 时间按 UTC），排序键为
  (有效时间, 包 seq, 包内原始位置)。

连续稳定段（候选序列上的连续子序列）须同时满足：
- 段内最大值 - 最小值 <= max_fluctuation（边界合格）；
- 任意相邻两条读数的有效时间差 <= max_gap_seconds（边界合格）；
  相邻间隔超限时序列在此处断开，稳定段不得跨断裂；
- 首末有效时间跨度 >= min_duration_seconds（边界合格）；
- 段内读数条数 >= min_readings。

阈值比较一律按请求给出的原始十进制阈值精确进行（Decimal），
不做任何舍入或额外容差：读数值与阈值直接由原始十进制串构造
Decimal，微秒间隔 / 时长（整数微秒）与阈值换算后的十进制微秒
比较——1.5 微秒的间隔上限不会放进 2 微秒的间隔，2.5 微秒的时长
下限不会放行 2 微秒的片段，max_fluctuation=0 也不放过任何非零
波动；同时 21.6 - 20.0 <= 1.6 这类十进制边界仍然成立。

求解（一次线性扫描，每个断裂块独立）：
- 以每条读数 j 为右端点，单调双端队列分别维护段内最小 / 最大读数窗口，
  得到满足波动约束的最小可行起点 L(j)（与断裂块起点取最大）；
- [L(j), j] 是以 j 为右端点满足波动与间隔约束的**最长**段：它在所有
  这类段中起点最早、读数最多、时间跨度最大，故只要存在任何以 j 为
  右端点的合格段（达到最少读数与最短时长），[L(j), j] 必然合格，且
  正是"同一完成时间取最早起点"所求的段；
- 右端点 j 按时间序升序扫描，第一个合格段即"最早完成"。
"""

import json
import sqlite3
from bisect import bisect_right
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Deque, List, Optional

from .schemas import WarmupReading, WarmupReleaseResponse, WarmupSegment

_US_PER_SECOND = 1_000_000


class WarmupError(ValueError):
    """放行判定参数非法（时间范围倒置、阈值为负、最少读数不足 1 等）。"""


@dataclass(frozen=True)
class _Point:
    """一条候选读数；下标即排序后的候选序号。"""

    seq: int
    position: int
    time: datetime
    time_us: int  # 自 UTC epoch 的微秒数，避免浮点秒比较误差
    time_source: str  # "ts" / "sent_at"
    value: Decimal  # 由原始 JSON 十进制串精确构造，避免二进制浮点表示误差
    unit: Optional[str]


def _as_utc(dt: datetime) -> datetime:
    """naive 时间一律视为 UTC，保证比较行为确定。"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_time(raw: str) -> datetime:
    return _as_utc(datetime.fromisoformat(raw))


def _to_us(dt: datetime) -> int:
    delta = dt - datetime.fromtimestamp(0, tz=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * _US_PER_SECOND + delta.microseconds


def _threshold_seconds_to_us(seconds: float) -> Decimal:
    """把秒级阈值按原始十进制值精确换算成微秒，不做任何舍入。

    str() 给出能往返还原该浮点数的最短十进制串，即请求中用户给出的
    原始阈值（如 0.0000015）；与整数微秒比较时 Decimal 精确参与，
    1.5 微秒的上限不会被舍入成 2 微秒。
    """
    return Decimal(str(seconds)) * _US_PER_SECOND


def _raw_value(raw) -> Decimal:
    """读数值统一为 Decimal：parse_float 已保证浮点 JSON 数字按原始
    十进制串构造；整数 JSON 数字（int）直接精确转换。"""
    return raw if isinstance(raw, Decimal) else Decimal(raw)


def load_readings(
    conn: sqlite3.Connection,
    device_id: str,
    metric: str,
    unit: str,
    from_: datetime,
    to: datetime,
):
    """读取设备在 [from, to) 内同指标、单位相符的候选读数及排除计数。

    仅扫描已提交包；测量时间优先取 ts、否则取包 sent_at，均无则忽略。
    返回 (候选点列表, 无有效时间排除数, 单位不符排除数)。
    """
    points: List[_Point] = []
    no_time = 0
    unit_mismatch = 0
    rows = conn.execute(
        "SELECT seq, payload_json FROM committed WHERE device_id = ? ORDER BY seq",
        (device_id,),
    ).fetchall()
    for row in rows:
        # parse_float=Decimal：读数值按原始十进制串精确构造，
        # 波动比较不受二进制浮点表示误差影响
        payload = json.loads(row["payload_json"], parse_float=Decimal)
        raw_sent_at = payload.get("sent_at")
        sent_at = _parse_time(raw_sent_at) if raw_sent_at else None
        for position, measurement in enumerate(payload.get("measurements", [])):
            if measurement.get("metric") != metric:
                continue
            raw_ts = measurement.get("ts")
            if raw_ts:
                time = _parse_time(raw_ts)
                time_source = "ts"
            elif sent_at is not None:
                time = sent_at
                time_source = "sent_at"
            else:
                no_time += 1  # 无 ts 且包无 sent_at：忽略
                continue
            if not (from_ <= time < to):
                continue  # 时间范围外：既不算候选也不算单位不符
            if measurement.get("unit") != unit:
                unit_mismatch += 1  # 单位不符（含缺单位）：不参与判定
                continue
            points.append(
                _Point(
                    seq=row["seq"],
                    position=position,
                    time=time,
                    time_us=_to_us(time),
                    time_source=time_source,
                    value=_raw_value(measurement["value"]),
                    unit=measurement.get("unit"),
                )
            )
    points.sort(key=lambda p: (p.time, p.seq, p.position))
    return points, no_time, unit_mismatch


def _build_segment(
    points: List[_Point],
    start: int,
    end: int,
) -> WarmupSegment:
    """根据候选下标闭区间 [start, end] 构造稳定段结果。"""
    window = points[start:end + 1]
    values = [p.value for p in window]
    min_value = min(values)
    max_value = max(values)
    max_gap_us = 0
    for k in range(1, len(window)):
        gap_us = window[k].time_us - window[k - 1].time_us
        if gap_us > max_gap_us:
            max_gap_us = gap_us
    readings = [
        WarmupReading(
            index=idx + 1,
            seq=p.seq,
            position=p.position,
            time=p.time,
            time_source=p.time_source,
            value=float(p.value),
            unit=p.unit,
        )
        for idx, p in enumerate(window)
    ]
    duration_us = window[-1].time_us - window[0].time_us
    return WarmupSegment(
        start_time=window[0].time,
        end_time=window[-1].time,
        duration_seconds=duration_us / _US_PER_SECOND,
        reading_count=len(window),
        min_value=float(min_value),
        max_value=float(max_value),
        fluctuation=float(max_value - min_value),
        max_adjacent_gap_seconds=max_gap_us / _US_PER_SECOND,
        readings=readings,
    )


def find_stable_segment(
    points: List[_Point],
    max_fluctuation: float,
    min_duration_seconds: float,
    min_readings: int,
    max_gap_seconds: float,
) -> Optional[WarmupSegment]:
    """在候选序列中寻找最早完成的合格连续稳定段；不存在返回 None。"""
    if not points:
        return None

    # 阈值按原始十进制值精确换算 / 构造，不舍入、不加额外容差：
    # 整数微秒间隔 / 时长与 Decimal 阈值直接比较（边界合格）
    min_duration_us = _threshold_seconds_to_us(min_duration_seconds)
    max_gap_us = _threshold_seconds_to_us(max_gap_seconds)
    max_fluctuation_d = Decimal(str(max_fluctuation))
    times = [p.time_us for p in points]

    # 单调队列窗口左沿；只在当前断裂块内移动，遇到间隔断裂时整体重置。
    window_lo = 0
    min_q: Deque[int] = deque()  # 值升序：队首为窗口内最小值下标
    max_q: Deque[int] = deque()  # 值降序：队首为窗口内最大值下标

    for j, point in enumerate(points):
        if j > 0 and point.time_us - points[j - 1].time_us > max_gap_us:
            # 相邻采样间隔超限：序列在此断开，稳定段不得跨断裂
            window_lo = j
            min_q.clear()
            max_q.clear()

        while min_q and points[min_q[-1]].value >= point.value:
            min_q.pop()
        min_q.append(j)
        while max_q and points[max_q[-1]].value <= point.value:
            max_q.pop()
        max_q.append(j)

        # 收缩窗口直至最大 - 最小不超过最大波动（边界合格；
        # 读数与阈值均为原始十进制精确值，不做任何额外放宽：
        # max_fluctuation=0 时任何非零波动都会触发收缩）
        while (
            points[max_q[0]].value - points[min_q[0]].value
            > max_fluctuation_d
        ):
            if min_q[0] == window_lo:
                min_q.popleft()
            if max_q[0] == window_lo:
                max_q.popleft()
            window_lo += 1

        # [window_lo, j] 是以 j 为右端点满足波动与间隔约束的最长段，
        # 其起点最早、读数最多、跨度最大：任何以 j 为右端点的合格段
        # 存在当且仅当它同时达到最少读数与最短时长。
        if j - window_lo + 1 < min_readings:
            continue  # 该块内读数条数尚不足

        duration_ok_at = bisect_right(
            times,
            points[j].time_us - min_duration_us,
            lo=window_lo,
            hi=j + 1,
        ) - 1
        if duration_ok_at >= window_lo:
            return _build_segment(points, window_lo, j)

    return None


def evaluate_warmup_release(
    conn: sqlite3.Connection,
    device_id: str,
    metric: str,
    unit: str,
    from_: datetime,
    to: datetime,
    max_fluctuation: float,
    min_duration_seconds: float,
    min_readings: int,
    max_gap_seconds: float,
) -> WarmupReleaseResponse:
    """执行仪器预热稳定放行判定。调用方需保证设备存在。"""
    from_ = _as_utc(from_)
    to = _as_utc(to)
    if from_ >= to:
        raise WarmupError("参数 from 必须早于 to")
    if max_fluctuation < 0:
        raise WarmupError("max_fluctuation 不能为负数")
    if min_duration_seconds <= 0:
        raise WarmupError("min_duration_seconds 必须为正数")
    if min_readings < 1:
        raise WarmupError("min_readings 至少为 1")
    if max_gap_seconds < 0:
        raise WarmupError("max_gap_seconds 不能为负数")

    points, no_time, unit_mismatch = load_readings(
        conn, device_id, metric, unit, from_, to
    )
    segment = find_stable_segment(
        points,
        max_fluctuation,
        min_duration_seconds,
        min_readings,
        max_gap_seconds,
    )

    not_released_reason = None
    if segment is None:
        not_released_reason = (
            f"时间范围内指标 {metric}、单位 {unit} 的 {len(points)} 条读数中"
            "不存在同时满足最大波动、最短持续时长、最少读数与最大相邻采样"
            "间隔的连续稳定段，不予放行"
        )

    return WarmupReleaseResponse(
        device_id=device_id,
        metric=metric,
        unit=unit,
        **{"from": from_},
        to=to,
        max_fluctuation=max_fluctuation,
        min_duration_seconds=min_duration_seconds,
        min_readings=min_readings,
        max_gap_seconds=max_gap_seconds,
        candidate_count=len(points),
        excluded_no_time_count=no_time,
        excluded_unit_mismatch_count=unit_mismatch,
        released=segment is not None,
        segment=segment,
        not_released_reason=not_released_reason,
    )
