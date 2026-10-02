import { agentRunSchema, chatMessageSchema, chatPageQuerySchema, conversationCreateSchema, conversationSchema, type ChatRunKind, type ChatSource, type NewsAnalysis, type SessionContinuity } from '@portfolio-pilot/contracts';
import type { PrismaClient } from './generated/prisma/client.js';
import { requireOwner, type AuthenticatedOwner } from './repositories.js';
import { PortfolioError } from './portfolio-service.js';

export function chatService(db: PrismaClient, owner: AuthenticatedOwner) {
  const ownerId = requireOwner(owner);
  const dto = (row: { createdAt: Date; updatedAt: Date }) => conversationSchema.parse({ ...row, createdAt: row.createdAt.toISOString(), updatedAt: row.updatedAt.toISOString() });
  const messageDto = (row: { createdAt: Date }) => chatMessageSchema.parse({ ...row, createdAt: row.createdAt.toISOString() });
  const runDto = (row: { id: string; conversationId: string; status: string; kind: string; userMessageId: string; assistantMessageId: string; cancelRequestedAt: Date | null; createdAt: Date; completedAt: Date | null }) =>
    agentRunSchema.parse({ id: row.id, conversationId: row.conversationId, status: row.status, kind: row.kind, userMessageId: row.userMessageId, assistantMessageId: row.assistantMessageId,
      cancelRequested: row.cancelRequestedAt !== null, createdAt: row.createdAt.toISOString(), completedAt: row.completedAt?.toISOString() ?? null });
  async function ownedRun(runId: string) {
    const row = await db.agentRun.findFirst({ where: { id: runId, conversation: { ownerId } } });
    if (!row) throw new PortfolioError(404, 'Resource not found.');
    return row;
  }
  /**
   * The only running -> terminal transition. The conditional update makes it exactly-once across
   * concurrent finishers (worker, cancel, stale reconciliation); the loser gets null and must not
   * announce a second outcome. The assistant message reuses the run's reserved application ID.
   */
  async function finishRun(runId: string, outcome: RunOutcome) {
    return db.$transaction(async tx => {
      const run = await tx.agentRun.findFirst({ where: { id: runId, conversation: { ownerId } } });
      if (!run) throw new PortfolioError(404, 'Resource not found.');
      const completedAt = new Date();
      const moved = await tx.agentRun.updateMany({ where: { id: runId, status: 'running' }, data: { status: outcome.status, failureCode: outcome.failureCode, completedAt, ...(outcome.attempts === undefined ? {} : { attempts: outcome.attempts }) } });
      if (!moved.count) return null;
      await tx.conversation.update({ where: { id: run.conversationId }, data: { updatedAt: completedAt } });
      const message = await tx.chatMessage.create({ data: { id: run.assistantMessageId, conversationId: run.conversationId, role: 'assistant', status: outcome.status,
        content: outcome.content, mode: outcome.mode, instructionVersion: outcome.instructionVersion, sources: outcome.sources, kind: run.kind,
        // Only a completed run may carry an analysis; failures never persist unvalidated structure.
        ...(outcome.status === 'completed' && outcome.analysis ? { analysis: outcome.analysis } : {}),
        ...(outcome.continuity ? { continuity: outcome.continuity } : {}) } });
      return { run: runDto({ ...run, status: outcome.status, completedAt }), message: messageDto(message) };
    });
  }
  async function get(id: string) {
    const row = await db.conversation.findFirst({ where: { id, ownerId } });
    if (!row) throw new PortfolioError(404, 'Resource not found.');
    return dto(row);
  }
  return {
    get,
    async create(input: unknown) {
      const data = conversationCreateSchema.parse(input);
      if (data.portfolioId && !await db.portfolio.findFirst({ where: { id: data.portfolioId, ownerId } })) throw new PortfolioError(404, 'Resource not found.');
      return dto(await db.conversation.create({ data: { ...data, ownerId } }));
    },
    async list(input: unknown) {
      const { limit, before } = chatPageQuerySchema.parse(input);
      const anchor = before ? await get(before) : null;
      const rows = await db.conversation.findMany({ where: { ownerId, ...(anchor ? { OR: [{ createdAt: { lt: new Date(anchor.createdAt) } }, { createdAt: new Date(anchor.createdAt), id: { lt: anchor.id } }] } : {}) }, orderBy: [{ createdAt: 'desc' }, { id: 'desc' }], take: limit + 1 });
      return { conversations: rows.slice(0, limit).map(dto), nextBefore: rows.length > limit ? rows[limit - 1]!.id : null };
    },
    async messages(id: string, input: unknown) {
      await get(id);
      const { limit, before } = chatPageQuerySchema.parse(input);
      const anchor = before ? await db.chatMessage.findFirst({ where: { id: before, conversationId: id, conversation: { ownerId } } }) : null;
      if (before && !anchor) throw new PortfolioError(404, 'Resource not found.');
      const rows = await db.chatMessage.findMany({ where: { conversationId: id, conversation: { ownerId }, ...(anchor ? { sequence: { lt: anchor.sequence } } : {}) }, orderBy: { sequence: 'desc' }, take: limit + 1 });
      return { messages: rows.slice(0, limit).reverse().map(messageDto), nextBefore: rows.length > limit ? rows[limit - 1]!.id : null };
    },
    async append(id: string, input: { role: 'user' | 'assistant'; content: string; status: 'completed' | 'failed'; mode: 'mock' | 'claude' | null; instructionVersion: string | null; sources: ChatSource[] }) {
      // Ownership is checked again inside the transaction; no naked message-by-ID writes.
      return db.$transaction(async tx => {
        const changed = await tx.conversation.updateMany({ where: { id, ownerId }, data: { updatedAt: new Date() } });
        if (!changed.count) throw new PortfolioError(404, 'Resource not found.');
        return messageDto(await tx.chatMessage.create({ data: { ...input, conversationId: id } }));
      });
    },
    /** Persists the user message and a running run atomically; a second running run in the conversation is a 409. */
    async startRun(id: string, input: { runId: string; assistantMessageId: string; content: string; kind?: ChatRunKind }) {
      try {
        return await db.$transaction(async tx => {
          const changed = await tx.conversation.updateMany({ where: { id, ownerId }, data: { updatedAt: new Date() } });
          if (!changed.count) throw new PortfolioError(404, 'Resource not found.');
          const kind = input.kind ?? 'answer';
          const user = await tx.chatMessage.create({ data: { conversationId: id, role: 'user', content: input.content, status: 'completed', mode: null, instructionVersion: null, sources: [], kind } });
          const run = await tx.agentRun.create({ data: { id: input.runId, conversationId: id, userMessageId: user.id, assistantMessageId: input.assistantMessageId, status: 'running', kind } });
          return { run: runDto(run), userMessage: messageDto(user) };
        });
      } catch (error) {
        if ((error as { code?: string }).code === 'P2002') throw new PortfolioError(409, 'This conversation is already answering. Wait for it or cancel it first.');
        throw error;
      }
    },
    finishRun,
    async getRun(runId: string) { return runDto(await ownedRun(runId)); },
    async activeRun(id: string) {
      await get(id);
      const row = await db.agentRun.findFirst({ where: { conversationId: id, status: 'running', conversation: { ownerId } } });
      return row ? runDto(row) : null;
    },
    /** Durable cancellation request; idempotent, and a no-op for a run that already finished. */
    async requestCancel(runId: string) {
      await ownedRun(runId);
      await db.agentRun.updateMany({ where: { id: runId, status: 'running', cancelRequestedAt: null }, data: { cancelRequestedAt: new Date() } });
      return runDto(await ownedRun(runId));
    },
    /**
     * The conversation's current SDK session binding, or null. The binding names a session; it does
     * not contain the transcript, which exists only in SDK session files on the host that wrote them.
     */
    async sessionBinding(id: string): Promise<SessionBinding | null> {
      const row = await db.conversationSession.findFirst({ where: { conversationId: id, conversation: { ownerId } } });
      return row ? { sdkSessionId: row.sdkSessionId, agentMode: row.agentMode as SessionBinding['agentMode'], hostKey: row.hostKey, instructionVersion: row.instructionVersion, modelKey: row.modelKey, generation: row.generation } : null;
    },
    /**
     * Compare-and-set: records `next` only if the binding is still at `expectedGeneration` (null =
     * none yet). Turns are serialized per conversation, so a lost race means another writer (for
     * example a run that outlived a restart) already moved the binding; the caller keeps theirs.
     */
    async bindSession(id: string, expectedGeneration: number | null, next: Omit<SessionBinding, 'generation'> & { runId: string }): Promise<boolean> {
      const data = { sdkSessionId: next.sdkSessionId, agentMode: next.agentMode, hostKey: next.hostKey, instructionVersion: next.instructionVersion, modelKey: next.modelKey, lastRunId: next.runId };
      return db.$transaction(async tx => {
        if (!await tx.conversation.findFirst({ where: { id, ownerId }, select: { id: true } })) throw new PortfolioError(404, 'Resource not found.');
        if (expectedGeneration === null) {
          const inserted = await tx.conversationSession.createMany({ data: [{ conversationId: id, ...data }], skipDuplicates: true });
          return inserted.count === 1;
        }
        const moved = await tx.conversationSession.updateMany({ where: { conversationId: id, generation: expectedGeneration }, data: { ...data, generation: { increment: 1 } } });
        return moved.count === 1;
      });
    },
    /** Owner's running runs created before `cutoff` (candidates for stale-run reconciliation). */
    async staleRuns(cutoff: Date) {
      return (await db.agentRun.findMany({ where: { status: 'running', createdAt: { lt: cutoff }, conversation: { ownerId } }, orderBy: { createdAt: 'asc' }, take: 20 })).map(runDto);
    }
  };
}
export type RunOutcome = {
  status: 'completed' | 'failed' | 'cancelled'; failureCode: string | null; content: string; mode: 'mock' | 'claude' | null; instructionVersion: string | null; sources: ChatSource[];
  analysis?: NewsAnalysis | null; continuity?: SessionContinuity | null; attempts?: number;
};
export type SessionBinding = { sdkSessionId: string; agentMode: 'mock' | 'claude'; hostKey: string; instructionVersion: string; modelKey: string; generation: number };
export type ChatService = ReturnType<typeof chatService>;
