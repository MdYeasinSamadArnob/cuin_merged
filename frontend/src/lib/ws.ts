/**
 * WebSocket Hook for Real-time Updates
 * 
 * Manages WebSocket connection to the backend for live pipeline updates.
 */

import { useEffect, useState, useCallback, useRef } from 'react';

const WS_URL = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/ws';

interface WebSocketMessage {
    type: string;
    data: any;
}

interface WebSocketHook {
    isConnected: boolean;
    lastMessage: WebSocketMessage | null;
    sendMessage: (message: string) => void;
}

export function useWebSocket(): WebSocketHook {
    const [isConnected, setIsConnected] = useState(false);
    const [lastMessage, setLastMessage] = useState<WebSocketMessage | null>(null);
    const wsRef = useRef<WebSocket | null>(null);
    const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);

    const connect = useCallback(() => {
        try {
            const ws = new WebSocket(WS_URL);

            ws.onopen = () => {
                console.log('WebSocket connected');
                setIsConnected(true);
                // Send ping to keep connection alive
                ws.send('ping');
            };

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    setLastMessage(data);
                } catch {
                    // Handle plain text messages like "pong"
                    if (event.data !== 'pong') {
                        console.log('WebSocket message:', event.data);
                    }
                }
            };

            ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

            ws.onclose = () => {
                console.log('WebSocket disconnected');
                setIsConnected(false);
                wsRef.current = null;

                // Attempt to reconnect after 5 seconds
                reconnectTimeoutRef.current = setTimeout(() => {
                    console.log('Attempting to reconnect WebSocket...');
                    connect();
                }, 5000);
            };

            wsRef.current = ws;
        } catch (error) {
            console.error('Failed to create WebSocket connection:', error);
        }
    }, []);

    const sendMessage = useCallback((message: string) => {
        if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
            wsRef.current.send(message);
        } else {
            console.warn('WebSocket is not connected. Cannot send message.');
        }
    }, []);

    useEffect(() => {
        connect();

        // Cleanup on unmount
        return () => {
            if (reconnectTimeoutRef.current) {
                clearTimeout(reconnectTimeoutRef.current);
            }
            if (wsRef.current) {
                wsRef.current.close();
            }
        };
    }, [connect]);

    // Periodic ping to keep connection alive
    useEffect(() => {
        if (!isConnected) return;

        const pingInterval = setInterval(() => {
            sendMessage('ping');
        }, 30000); // Ping every 30 seconds

        return () => clearInterval(pingInterval);
    }, [isConnected, sendMessage]);

    return {
        isConnected,
        lastMessage,
        sendMessage,
    };
}
