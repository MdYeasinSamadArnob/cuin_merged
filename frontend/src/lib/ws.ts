import { useState, useEffect, useRef } from 'react';

const WS_URL = process.env.NEXT_PUBLIC_WS_URL;
const API_URL = process.env.NEXT_PUBLIC_API_URL;
type WebSocketEventPayload = {
    type?: string;
    data?: Record<string, unknown> & { error?: string };
    payload?: Record<string, unknown>;
    [key: string]: unknown;
};

function toWsUrl(httpUrl: string, path = '/ws'): string {
    const parsed = new URL(httpUrl);
    // Normalise http(s) → ws(s) so mis-configured env vars don't break things
    if (parsed.protocol === 'https:' || parsed.protocol === 'wss:') {
        parsed.protocol = 'wss:';
    } else {
        parsed.protocol = 'ws:';
    }
    parsed.pathname = path;
    parsed.search = '';
    parsed.hash = '';
    return parsed.toString();
}

function buildWsUrl(): string {
    if (WS_URL) {
        // Handle env vars that mistakenly use http(s) instead of ws(s)
        return toWsUrl(WS_URL);
    }

    if (API_URL) {
        return toWsUrl(API_URL);
    }

    if (typeof window !== 'undefined') {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const host = window.location.hostname;
        const port = window.location.port === '3000' || window.location.port === '3001'
            ? '8000'
            : window.location.port;
        return `${protocol}//${host}${port ? `:${port}` : ''}/ws`;
    }

    return 'ws://localhost:8000/ws';
}

export function useWebSocket() {
    const [isConnected, setIsConnected] = useState(false);
    const [lastEvent, setLastEvent] = useState<WebSocketEventPayload | null>(null);
    const ws = useRef<WebSocket | null>(null);
    const reconnectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
    const reconnectAttempts = useRef(0);
    const MAX_RETRIES = 5;

    useEffect(() => {
        let disposed = false;
        const wsUrl = buildWsUrl();

        const connect = () => {
            if (disposed) return;
            if (reconnectAttempts.current >= MAX_RETRIES) {
                console.warn(`WebSocket: giving up after ${MAX_RETRIES} failed attempts.`, { url: wsUrl });
                return;
            }
            const socket = new WebSocket(wsUrl);
            ws.current = socket;

            socket.onopen = () => {
                reconnectAttempts.current = 0;
                setIsConnected(true);
            };


            socket.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    setLastEvent(data);
                } catch (err) {
                    console.error('Failed to parse WS message:', err);
                }
            };

            socket.onclose = (event) => {
                setIsConnected(false);
                if (disposed) return;
                const delayMs = Math.min(1000 * (2 ** reconnectAttempts.current), 10000);
                reconnectAttempts.current += 1;
                reconnectTimer.current = setTimeout(connect, delayMs);
                if (!event.wasClean) {
                    console.warn('WebSocket closed unexpectedly', {
                        url: wsUrl,
                        code: event.code,
                        reason: event.reason || 'none',
                    });
                }
            };

            socket.onerror = () => {
                console.error('WebSocket Error', { url: wsUrl, readyState: socket.readyState });
            };
        };

        connect();

        return () => {
            disposed = true;
            if (reconnectTimer.current) {
                clearTimeout(reconnectTimer.current);
                reconnectTimer.current = null;
            }
            if (ws.current && (ws.current.readyState === WebSocket.OPEN || ws.current.readyState === WebSocket.CONNECTING)) {
                ws.current.close();
            }
        };
    }, []);

    return { isConnected, lastEvent };
}
