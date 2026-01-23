'use client';

import { useChatContext } from '@/contexts/ChatContext';
import { getApiBaseUrl } from '@/lib/constants';
import FloatingChat from './FloatingChat';

interface ChatWrapperProps {
  apiBase?: string;
}

export default function ChatWrapper({ apiBase }: ChatWrapperProps) {
  const { currentIncident } = useChatContext();

  // Use centralized API URL if not provided
  const effectiveApiBase = apiBase || getApiBaseUrl();

  return (
    <FloatingChat
      apiBase={effectiveApiBase}
      currentIncident={currentIncident || undefined}
    />
  );
}
