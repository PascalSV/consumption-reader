import { getTimes } from "suncalc";

export interface Env {
    "TASMOTA-READINGS": D1Database;
    API_KEY?: string;
    TASMOTA_ANALYTICS_IP_ALLOWLIST?: string;
    TASMOTA_ANALYTICS_TIMEZONE?: string;
}

interface RequestLogContext {
    cfRay: string | null;
    sourceIp: string | null;
    method: string;
    path: string;
}

interface ReadingRow {
    received_at: string;
    input_kwh: number;
}

interface NightlyAggregate {
    kwh: number;
    points: number;
}

interface NightlyStats {
    n: number;
    avg: number;
    med: number;
    p90: number;
    p95: number;
    min: number;
    max: number;
}

interface NightlyResult {
    d: string[];
    k: number[];
    p: number[];
    s: NightlyStats;
    ss: {
        w: NightlyStats;
        s: NightlyStats;
        t: NightlyStats;
    };
}

interface LocalDateTimeParts {
    date: string;
    hour: number;
    minute: number;
    minuteOfDay: number;
}

interface NightWindowConfig {
    model: "clock" | "solar";
    startHour: number;
    endHour: number;
    latitude: number | null;
    longitude: number | null;
    sunsetOffsetMin: number;
    sunriseOffsetMin: number;
    days: number;
    minPoints: number;
    maxNights: number;
    timeZone: string;
}

interface SolarThreshold {
    sunsetStartMinute: number;
    sunriseEndMinute: number;
}

const ANALYTICS_PATH = "/analytics/nightly";
const DEFAULT_ANALYTICS_TIMEZONE = "Europe/Berlin";

function notFoundResponse(): Response {
    return new Response("Not Found", { status: 404 });
}

function methodNotAllowedResponse(): Response {
    return new Response("Method Not Allowed", {
        status: 405,
        headers: {
            Allow: "GET",
        },
    });
}

function badRequestResponse(message: string): Response {
    return new Response(message, { status: 400 });
}

function jsonNoStore(payload: unknown, status = 200): Response {
    return new Response(JSON.stringify(payload), {
        status,
        headers: {
            "content-type": "application/json; charset=utf-8",
            "cache-control": "no-store, max-age=0",
            "x-content-type-options": "nosniff",
            "referrer-policy": "no-referrer",
        },
    });
}

function getRequestLogContext(request: Request, url: URL): RequestLogContext {
    return {
        cfRay: request.headers.get("cf-ray"),
        sourceIp: request.headers.get("CF-Connecting-IP"),
        method: request.method,
        path: url.pathname,
    };
}

function toFiniteNumber(value: unknown): number | null {
    if (typeof value === "number") {
        return Number.isFinite(value) ? value : null;
    }

    if (typeof value === "string") {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    }

    return null;
}

function formatUtcTimestamp(date: Date): string {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");
    const hours = String(date.getUTCHours()).padStart(2, "0");
    const minutes = String(date.getUTCMinutes()).padStart(2, "0");
    const seconds = String(date.getUTCSeconds()).padStart(2, "0");

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} UTC`;
}

function parseIntegerParam(
    value: string | null,
    defaultValue: number,
    min: number,
    max: number
): number {
    if (!value) {
        return defaultValue;
    }

    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed)) {
        return defaultValue;
    }

    if (parsed < min) {
        return min;
    }

    if (parsed > max) {
        return max;
    }

    return parsed;
}

function parseFloatParam(value: string | null): number | null {
    if (!value) {
        return null;
    }

    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function parseBearerToken(request: Request): string | null {
    const authHeader = request.headers.get("Authorization");
    if (!authHeader) {
        return null;
    }

    const match = authHeader.match(/^Bearer\s+(.+)$/i);
    if (!match) {
        return null;
    }

    return match[1].trim();
}

function timingSafeEqual(a: string, b: string): boolean {
    const aBytes = new TextEncoder().encode(a);
    const bBytes = new TextEncoder().encode(b);

    if (aBytes.length !== bBytes.length) {
        return false;
    }

    let mismatch = 0;
    for (let index = 0; index < aBytes.length; index += 1) {
        mismatch |= aBytes[index] ^ bBytes[index];
    }

    return mismatch === 0;
}

function parseIpAllowlist(raw: string | undefined): Set<string> {
    if (!raw) {
        return new Set();
    }

    return new Set(
        raw
            .split(",")
            .map((entry) => entry.trim())
            .filter((entry) => entry.length > 0)
    );
}

function isAnalyticsAuthorized(
    request: Request,
    env: Env,
    requestContext: RequestLogContext
): boolean {
    const configuredToken = env.API_KEY?.trim();
    if (!configuredToken) {
        console.error("Analytics endpoint rejected due to missing API_KEY", requestContext);
        return false;
    }

    const providedToken = parseBearerToken(request);
    if (!providedToken || !timingSafeEqual(providedToken, configuredToken)) {
        console.warn("Analytics request rejected", {
            ...requestContext,
            reason: "token_invalid",
        });
        return false;
    }

    const allowlist = parseIpAllowlist(env.TASMOTA_ANALYTICS_IP_ALLOWLIST);
    if (allowlist.size === 0) {
        return true;
    }

    const sourceIp = request.headers.get("CF-Connecting-IP")?.trim() ?? null;
    if (!sourceIp || !allowlist.has(sourceIp)) {
        console.warn("Analytics request rejected", {
            ...requestContext,
            reason: "ip_not_allowed",
        });
        return false;
    }

    return true;
}

function parseReceivedAtUtc(value: string): Date | null {
    const trimmed = value.trim();
    if (!trimmed.endsWith(" UTC")) {
        return null;
    }

    const isoValue = `${trimmed.slice(0, -4).replace(" ", "T")}Z`;
    const parsed = new Date(isoValue);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function createLocalDateTimeFormatter(timeZone: string): Intl.DateTimeFormat {
    return new Intl.DateTimeFormat("en-CA", {
        timeZone,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
    });
}

function getLocalDateTime(
    date: Date,
    formatter: Intl.DateTimeFormat
): LocalDateTimeParts | null {
    const parts = formatter.formatToParts(date);
    const year = parts.find((part) => part.type === "year")?.value;
    const month = parts.find((part) => part.type === "month")?.value;
    const day = parts.find((part) => part.type === "day")?.value;
    const hourValue = parts.find((part) => part.type === "hour")?.value;
    const minuteValue = parts.find((part) => part.type === "minute")?.value;

    if (!year || !month || !day || !hourValue || !minuteValue) {
        return null;
    }

    const hour = Number.parseInt(hourValue, 10);
    const minute = Number.parseInt(minuteValue, 10);
    if (!Number.isFinite(hour) || !Number.isFinite(minute)) {
        return null;
    }

    return {
        date: `${year}-${month}-${day}`,
        hour,
        minute,
        minuteOfDay: hour * 60 + minute,
    };
}

function resolveAnalyticsTimezone(env: Env): string {
    const configured = env.TASMOTA_ANALYTICS_TIMEZONE?.trim();
    const candidate = configured && configured.length > 0 ? configured : DEFAULT_ANALYTICS_TIMEZONE;

    try {
        new Intl.DateTimeFormat("en-US", { timeZone: candidate }).format(new Date());
        return candidate;
    } catch {
        return DEFAULT_ANALYTICS_TIMEZONE;
    }
}

function roundTo3(value: number): number {
    return Math.round(value * 1000) / 1000;
}

function computeStats(values: number[]): NightlyStats {
    if (values.length === 0) {
        return {
            n: 0,
            avg: 0,
            med: 0,
            p90: 0,
            p95: 0,
            min: 0,
            max: 0,
        };
    }

    const sorted = [...values].sort((a, b) => a - b);
    const n = sorted.length;
    const average = sorted.reduce((sum, value) => sum + value, 0) / n;
    const median = sorted[Math.floor((n - 1) / 2)];
    const p90 = sorted[Math.min(n - 1, Math.max(0, Math.ceil(n * 0.9) - 1))];
    const p95 = sorted[Math.min(n - 1, Math.max(0, Math.ceil(n * 0.95) - 1))];

    return {
        n,
        avg: roundTo3(average),
        med: roundTo3(median),
        p90: roundTo3(p90),
        p95: roundTo3(p95),
        min: roundTo3(sorted[0]),
        max: roundTo3(sorted[n - 1]),
    };
}

function addDaysIsoDate(dateIso: string, offsetDays: number): string {
    const year = Number.parseInt(dateIso.slice(0, 4), 10);
    const month = Number.parseInt(dateIso.slice(5, 7), 10);
    const day = Number.parseInt(dateIso.slice(8, 10), 10);

    const date = new Date(Date.UTC(year, month - 1, day));
    date.setUTCDate(date.getUTCDate() + offsetDays);
    return date.toISOString().slice(0, 10);
}

function clampMinuteOfDay(value: number): number {
    if (value < 0) {
        return 0;
    }

    if (value > 1439) {
        return 1439;
    }

    return value;
}

function getSeasonKey(dateIso: string): "w" | "s" | "t" {
    const month = Number.parseInt(dateIso.slice(5, 7), 10);

    if (month === 11 || month === 12 || month === 1 || month === 2) {
        return "w";
    }

    if (month >= 5 && month <= 8) {
        return "s";
    }

    return "t";
}

function computeSeasonStats(
    nightly: Array<[string, NightlyAggregate]>
): { w: NightlyStats; s: NightlyStats; t: NightlyStats } {
    const winter: number[] = [];
    const summer: number[] = [];
    const transition: number[] = [];

    for (const [dateIso, aggregate] of nightly) {
        const seasonKey = getSeasonKey(dateIso);
        if (seasonKey === "w") {
            winter.push(aggregate.kwh);
            continue;
        }

        if (seasonKey === "s") {
            summer.push(aggregate.kwh);
            continue;
        }

        transition.push(aggregate.kwh);
    }

    return {
        w: computeStats(winter),
        s: computeStats(summer),
        t: computeStats(transition),
    };
}

function parseNightWindowConfig(
    url: URL,
    timeZone: string
): NightWindowConfig | string {
    const modelRaw = url.searchParams.get("model")?.toLowerCase();
    const model = modelRaw === "solar" ? "solar" : "clock";

    const days = parseIntegerParam(url.searchParams.get("days"), 400, 7, 2000);
    const minPoints = parseIntegerParam(url.searchParams.get("minPoints"), 3, 1, 96);
    const maxNights = parseIntegerParam(url.searchParams.get("limit"), 366, 1, 2000);

    const startHour = parseIntegerParam(url.searchParams.get("startHour"), 22, 0, 23);
    const endHour = parseIntegerParam(url.searchParams.get("endHour"), 6, 0, 23);

    const latitude = parseFloatParam(url.searchParams.get("lat"));
    const longitude = parseFloatParam(url.searchParams.get("lon"));
    const sunsetOffsetMin = parseIntegerParam(url.searchParams.get("sunsetOffsetMin"), 0, -180, 180);
    const sunriseOffsetMin = parseIntegerParam(url.searchParams.get("sunriseOffsetMin"), 0, -180, 180);

    if (model === "solar") {
        if (latitude === null || longitude === null) {
            return "For model=solar both lat and lon are required";
        }

        if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            return "lat/lon out of range";
        }
    }

    return {
        model,
        startHour,
        endHour,
        latitude,
        longitude,
        sunsetOffsetMin,
        sunriseOffsetMin,
        days,
        minPoints,
        maxNights,
        timeZone,
    };
}

function getNightBucketForClock(local: LocalDateTimeParts, config: NightWindowConfig): string | null {
    const startMinute = config.startHour * 60;
    const endMinute = config.endHour * 60;

    if (startMinute === endMinute) {
        return local.date;
    }

    if (startMinute < endMinute) {
        return local.minuteOfDay >= startMinute && local.minuteOfDay < endMinute ? local.date : null;
    }

    if (local.minuteOfDay >= startMinute) {
        return local.date;
    }

    if (local.minuteOfDay < endMinute) {
        return addDaysIsoDate(local.date, -1);
    }

    return null;
}

function getSolarThreshold(
    dateIso: string,
    config: NightWindowConfig,
    formatter: Intl.DateTimeFormat,
    cache: Map<string, SolarThreshold>
): SolarThreshold | null {
    const cached = cache.get(dateIso);
    if (cached) {
        return cached;
    }

    const year = Number.parseInt(dateIso.slice(0, 4), 10);
    const month = Number.parseInt(dateIso.slice(5, 7), 10);
    const day = Number.parseInt(dateIso.slice(8, 10), 10);

    const referenceDate = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
    const solarTimes = getTimes(referenceDate, config.latitude ?? 0, config.longitude ?? 0);

    const sunriseLocal = getLocalDateTime(solarTimes.sunrise, formatter);
    const sunsetLocal = getLocalDateTime(solarTimes.sunset, formatter);
    if (!sunriseLocal || !sunsetLocal) {
        return null;
    }

    const threshold = {
        sunsetStartMinute: clampMinuteOfDay(sunsetLocal.minuteOfDay + config.sunsetOffsetMin),
        sunriseEndMinute: clampMinuteOfDay(sunriseLocal.minuteOfDay + config.sunriseOffsetMin),
    };

    cache.set(dateIso, threshold);
    return threshold;
}

function getNightBucketForSolar(
    local: LocalDateTimeParts,
    config: NightWindowConfig,
    formatter: Intl.DateTimeFormat,
    cache: Map<string, SolarThreshold>
): string | null {
    if (config.latitude === null || config.longitude === null) {
        return null;
    }

    const threshold = getSolarThreshold(local.date, config, formatter, cache);
    if (!threshold) {
        return null;
    }

    if (local.minuteOfDay >= threshold.sunsetStartMinute) {
        return local.date;
    }

    if (local.minuteOfDay < threshold.sunriseEndMinute) {
        return addDaysIsoDate(local.date, -1);
    }

    return null;
}

async function getNightlyAnalytics(
    env: Env,
    config: NightWindowConfig
): Promise<NightlyResult> {
    const end = new Date();
    const start = new Date(end.getTime() - config.days * 24 * 60 * 60 * 1000);

    const queryResult = await env["TASMOTA-READINGS"]
        .prepare(
            `SELECT received_at, input_kwh
       FROM tasmota_readings
      WHERE received_at >= ?1 AND received_at <= ?2
      ORDER BY received_at ASC`
        )
        .bind(formatUtcTimestamp(start), formatUtcTimestamp(end))
        .all<ReadingRow>();

    const rows = queryResult.results ?? [];
    const formatter = createLocalDateTimeFormatter(config.timeZone);
    const nightlyMap = new Map<string, NightlyAggregate>();
    const solarCache = new Map<string, SolarThreshold>();
    let previousInput: number | null = null;

    for (const row of rows) {
        const input = toFiniteNumber(row.input_kwh);
        if (input === null) {
            continue;
        }

        if (previousInput === null) {
            previousInput = input;
            continue;
        }

        const delta = input - previousInput;
        previousInput = input;

        if (!Number.isFinite(delta) || delta < 0) {
            continue;
        }

        const timestamp = parseReceivedAtUtc(row.received_at);
        if (!timestamp) {
            continue;
        }

        const localDateTime = getLocalDateTime(timestamp, formatter);
        if (!localDateTime) {
            continue;
        }

        const bucketDate =
            config.model === "solar"
                ? getNightBucketForSolar(localDateTime, config, formatter, solarCache)
                : getNightBucketForClock(localDateTime, config);

        if (!bucketDate) {
            continue;
        }

        const aggregate = nightlyMap.get(bucketDate) ?? { kwh: 0, points: 0 };
        aggregate.kwh += delta;
        aggregate.points += 1;
        nightlyMap.set(bucketDate, aggregate);
    }

    const allNights = Array.from(nightlyMap.entries())
        .filter(([, aggregate]) => aggregate.points >= config.minPoints)
        .sort(([leftDate], [rightDate]) => leftDate.localeCompare(rightDate));

    const selectedNights = allNights.slice(-config.maxNights);
    const dates = selectedNights.map(([date]) => date);
    const values = selectedNights.map(([, aggregate]) => roundTo3(aggregate.kwh));
    const points = selectedNights.map(([, aggregate]) => aggregate.points);
    const roundedNights: Array<[string, NightlyAggregate]> = selectedNights.map(([date, aggregate]) => [
        date,
        {
            kwh: roundTo3(aggregate.kwh),
            points: aggregate.points,
        },
    ]);

    return {
        d: dates,
        k: values,
        p: points,
        s: computeStats(values),
        ss: computeSeasonStats(roundedNights),
    };
}

async function handleNightlyAnalytics(
    request: Request,
    env: Env,
    url: URL,
    requestContext: RequestLogContext
): Promise<Response> {
    if (!isAnalyticsAuthorized(request, env, requestContext)) {
        return notFoundResponse();
    }

    if (request.method !== "GET") {
        return methodNotAllowedResponse();
    }

    const timeZone = resolveAnalyticsTimezone(env);
    const configOrError = parseNightWindowConfig(url, timeZone);
    if (typeof configOrError === "string") {
        return badRequestResponse(configOrError);
    }

    try {
        const nightly = await getNightlyAnalytics(env, configOrError);
        const payload: Record<string, unknown> = {
            v: 1,
            tz: timeZone,
            ...nightly,
            m: configOrError.model,
        };

        if (configOrError.model === "clock") {
            payload.w = [configOrError.startHour, configOrError.endHour];
        } else {
            payload.sl = {
                lat: configOrError.latitude,
                lon: configOrError.longitude,
                o: [configOrError.sunsetOffsetMin, configOrError.sunriseOffsetMin],
            };
        }

        return jsonNoStore(payload);
    } catch (error) {
        console.error("Failed to calculate nightly analytics", {
            ...requestContext,
            error: error instanceof Error ? error.message : String(error),
        });
        return new Response("Database read failed", { status: 500 });
    }
}

export default {
    async fetch(request: Request, env: Env): Promise<Response> {
        const url = new URL(request.url);
        const requestContext = getRequestLogContext(request, url);

        if (url.pathname === "/health") {
            return Response.json({ ok: true });
        }

        if (url.pathname === ANALYTICS_PATH) {
            return handleNightlyAnalytics(request, env, url, requestContext);
        }

        console.warn("Unhandled request", requestContext);
        return notFoundResponse();
    },
};
