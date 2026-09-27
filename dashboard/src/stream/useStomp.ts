import { Client } from "@stomp/stompjs";
import { useEffect, useRef, useState } from "react";

export type StreamStatus = "connecting" | "live" | "reconnecting" | "offline";

/** The subset of a STOMP client the hook uses; tests pass a fake. */
export interface StompLike {
  onConnect: (frame: unknown) => void;
  onWebSocketClose: (event: unknown) => void;
  onStompError: (frame: unknown) => void;
  subscribe: (destination: string, callback: (message: { body: string }) => void) => unknown;
  activate: () => void;
  deactivate: () => unknown;
}

export function createStompClient(url: string): StompLike {
  return new Client({
    brokerURL: url,
    reconnectDelay: 2000,
    heartbeatIncoming: 10000,
    heartbeatOutgoing: 10000,
  }) as unknown as StompLike;
}

/**
 * The live stream (spec §15.9): subscribes to the dashboard API's STOMP topics, reconnects
 * automatically (the client retries every 2 s), and reports its state for the connection
 * indicator.
 */
export function useStomp(
  url: string,
  topics: string[],
  onMessage: (topic: string, items: unknown[]) => void,
  createClient: (url: string) => StompLike = createStompClient,
): { status: StreamStatus; reconnects: number } {
  const [status, setStatus] = useState<StreamStatus>("connecting");
  const [reconnects, setReconnects] = useState(0);
  const handler = useRef(onMessage);
  handler.current = onMessage;
  const topicKey = topics.join(",");

  useEffect(() => {
    const client = createClient(url);
    let everConnected = false;
    client.onConnect = () => {
      if (everConnected) setReconnects((n) => n + 1);
      everConnected = true;
      setStatus("live");
      for (const topic of topicKey.split(",")) {
        client.subscribe(`/topic/${topic}`, (message) => {
          try {
            const body = JSON.parse(message.body);
            handler.current(topic, Array.isArray(body) ? body : [body]);
          } catch {
            // not JSON: ignored
          }
        });
      }
    };
    client.onWebSocketClose = () => setStatus(everConnected ? "reconnecting" : "offline");
    client.onStompError = () => setStatus("reconnecting");
    client.activate();
    return () => {
      client.deactivate();
    };
  }, [url, topicKey, createClient]);

  return { status, reconnects };
}
