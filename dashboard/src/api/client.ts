// The dashboard API (prompt 22): VITE_API_URL, default http://localhost:8080.
export const API_URL: string =
  (import.meta.env.VITE_API_URL as string | undefined) ?? "http://localhost:8080";

export const WS_URL: string = API_URL.replace(/^http/, "ws") + "/ws/stream";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public body?: unknown,
  ) {
    super(message);
  }
}

const KEY_STORAGE = "predisched.adminKey";

/** The admin key for control endpoints; kept in this browser only. */
export function adminKey(): string {
  try {
    return localStorage.getItem(KEY_STORAGE) ?? "dev-admin";
  } catch {
    return "dev-admin";
  }
}

export function setAdminKey(key: string) {
  try {
    localStorage.setItem(KEY_STORAGE, key);
  } catch {
    // private mode: the default stays
  }
}

async function parse(response: Response): Promise<unknown> {
  const text = await response.text();
  try {
    return text ? JSON.parse(text) : null;
  } catch {
    return text;
  }
}

export async function getJson<T>(path: string): Promise<T> {
  const response = await fetch(API_URL + path);
  const body = await parse(response);
  if (!response.ok) {
    throw new ApiError(response.status, errorText(body, response.status), body);
  }
  return body as T;
}

/** POST; {@code admin} adds the X-Admin-Key header. Refusals (409) resolve with ok=false. */
export async function postJson<T>(path: string, body?: unknown, admin = false): Promise<T> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (admin) headers["X-Admin-Key"] = adminKey();
  const response = await fetch(API_URL + path, {
    method: "POST",
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const parsed = await parse(response);
  if (!response.ok && response.status !== 409) {
    throw new ApiError(response.status, errorText(parsed, response.status), parsed);
  }
  return parsed as T;
}

function errorText(body: unknown, status: number): string {
  if (body && typeof body === "object") {
    const b = body as Record<string, unknown>;
    return String(b.detail ?? b.error ?? b.message ?? `HTTP ${status}`);
  }
  return `HTTP ${status}`;
}
