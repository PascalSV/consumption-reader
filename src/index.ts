export interface Env {
    "TASMOTA-READINGS": D1Database;
    TASMOTA_ANALYTICS_TOKEN?: string;
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
    const configuredToken = env.TASMOTA_ANALYTICS_TOKEN?.trim();
    if (!configuredToken) {
        console.error("Analytics endpoint rejected due to missing config", requestContext);
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

function createLocalDateHourFormatter(timeZone: string): Intl.DateTimeFormat {
    return new Intl.DateTimeFormat("en-CA", {
        timeZone,
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        hourCycle: "h23",
    });
}

function getLocalDateHour(
    date: Date,
    formatter: Intl.DateTimeFormat
): { date: string; hour: number } | null {
    const parts = formatter.formatToParts(date);
    const year = parts.find((part) => part.type === "year")?.value;
    const month = parts.find((part) => part.type === "month")?.value;
    const day = parts.find((part) => part.type === "day")?.value;
    const hourValue = parts.find((part) => part.type === "hour")?.value;

    if (!year || !month || !day || !hourValue) {
        return null;
    }

    const hour = Number.parseInt(hourValue, 10);
    if (!Number.isFinite(hour)) {
        return null;
    }

    return {
        date: `${year}-${month}-${day}`,
        hour,
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

async function getNightlyAnalytics(
    env: Env,
    days: number,
    minPoints: number,
    maxNights: number,
    timeZone: string
): Promise<{ d: string[]; k: number[]; p: number[]; s: NightlyStats }> {
    const end = new Date();
    const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);

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
    const formatter = createLocalDateHourFormatter(timeZone);
    const nightlyMap = new Map<string, NightlyAggregate>();
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

        const localDateHour = getLocalDateHour(timestamp, formatter);
        if (!localDateHour) {
            continue;
        }

        const inNightWindow = localDateHour.hour >= 22 || localDateHour.hour < 6;
        if (!inNightWindow) {
            continue;
        }

        const bucketDate =
            localDateHour.hour < 6
                ? getLocalDateHour(new Date(timestamp.getTime() - 6 * 60 * 60 * 1000), formatter)?.date
                : localDateHour.date;

        if (!bucketDate) {
            continue;
        }

        const aggregate = nightlyMap.get(bucketDate) ?? { kwh: 0, points: 0 };
        aggregate.kwh += delta;
        aggregate.points += 1;
        nightlyMap.set(bucketDate, aggregate);
    }

    const allNights = Array.from(nightlyMap.entries())
        .filter(([, aggregate]) => aggregate.points >= minPoints)
        .sort(([leftDate], [rightDate]) => leftDate.localeCompare(rightDate));

    const selectedNights = allNights.slice(-maxNights);
    const dates = selectedNights.map(([date]) => date);
    const values = selectedNights.map(([, aggregate]) => roundTo3(aggregate.kwh));
    const points = selectedNights.map(([, aggregate]) => aggregate.points);

    return {
        d: dates,
        k: values,
        p: points,
        s: computeStats(values),
    };
}

async function handleNightlyAnalytics(
    request: Request,
    env: Env,
    url: URL,
    requestContext: RequestLogContext
): Promise<Response> {
    if (request.method !== "GET") {
        return methodNotAllowedResponse();
    }

    if (!isAnalyticsAuthorized(request, env, requestContext)) {
        return notFoundResponse();
    }

    const days = parseIntegerParam(url.searchParams.get("days"), 400, 7, 2000);
    const minPoints = parseIntegerParam(url.searchParams.get("minPoints"), 3, 1, 48);
    const maxNights = parseIntegerParam(url.searchParams.get("limit"), 366, 1, 2000);
    const timeZone = resolveAnalyticsTimezone(env);

    try {
        const nightly = await getNightlyAnalytics(env, days, minPoints, maxNights, timeZone);
        return jsonNoStore({
            v: 1,
            tz: timeZone,
            w: [22, 6],
            ...nightly,
        });
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
