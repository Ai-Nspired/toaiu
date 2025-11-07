import { DurableObject } from "cloudflare:workers";

// CORS headers
const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Authorization",
  "Content-Type": "application/json"
};

// Model configurations
const MODEL_LIST = [
  { key: "dolphin-mistral-24b-venice-edition", model: "cognitivecomputations/dolphin-mistral-24b-venice-edition:free", requiresChallenge: true, challengeQuestion: "What is the passphrase?" },
  { key: "llama-3-8b", model: "meta-llama/llama-3-8b-instruct", requiresChallenge: false },
  { key: "llama-3.2-1b", model: "meta-llama/llama-3.2-1b-instruct", requiresChallenge: false },
  { key: "mistral-nemo", model: "mistralai/mistral-nemo", requiresChallenge: true, challengeQuestion: "What is the passphrase?" },
  { key: "qwen2.5-7b-instruct", model: "qwen/qwen2.5-7b-instruct", requiresChallenge: true, challengeQuestion: "What is the passphrase?" },
  { key: "gpt-oss-20b", model: "openai/gpt-oss-20b", requiresChallenge: true, challengeQuestion: "What is the passphrase?", paid: true }
];

const MODELS = {};
MODEL_LIST.forEach((m) => (MODELS[m.key] = m));

// ======================= KV HANDLER (dynamic truths) =======================

const KVHandler = {
  normalizeQuery(query) {
    if (!query?.trim()) return "";
    let normalized = query
      .trim()
      .toLowerCase()
      .replace(/[^\w\s]/g, "")
      .replace(/\s+/g, " ");

    const stemRules = {
      "\\brun(?:ning|s|n)?\\b": "run",
      "\\bprogram(?:ming|med)?\\b": "program",
      "\\bcode(?:d|ing)?\\b": "code",
      "\\bdevelop(?:ing|ed|ment)?\\b": "develop",
      "\\boptimiz(?:e|ation|ing|ed)?\\b": "optimize",
      "\\bcreat(?:e|ing|ed|ion)?\\b": "create",
      "\\bdebug(?:ging|ged)?\\b": "debug"
    };
    Object.keys(stemRules).forEach((rule) => {
      const regex = new RegExp(rule, "gi");
      normalized = normalized.replace(regex, stemRules[rule]);
    });

    normalized = normalized.replace(/\s+/g, "-").substring(0, 100);

    if (normalized.length > 50) {
      const encoder = new TextEncoder();
      let hash = 0;
      for (const char of encoder.encode(query)) {
        hash = ((hash << 5) - hash + char) & 0xffffffff;
      }
      const shortHash = hash.toString(16).substring(0, 8);
      normalized = normalized.substring(0, 80) + "-" + shortHash;
    }

    return normalized || "unknown-query";
  },

  buildCacheKey(input) {
    return `truth:${this.normalizeQuery(input)}`;
  },

  async withRetry(operation, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        if (attempt === maxRetries) throw error;
        console.warn(`KV retry ${attempt}/${maxRetries}:`, error.message);
        await new Promise((r) =>
          setTimeout(r, 100 * 2 ** (attempt - 1))
        );
      }
    }
  },

  async getTruth(key, env) {
    try {
      const cached = await env.memy.get(key);
      return cached ? JSON.parse(cached) : null;
    } catch (error) {
      console.error("KV get error:", error);
      return null;
    }
  },

  async setTruth(key, data, env, isProvisional = false, ttl = 3600) {
    try {
      await this.withRetry(async () => {
        const actualKey = isProvisional
          ? `truth:provisional:${key.replace("truth:", "")}`
          : key;

        await env.memy.put(actualKey, JSON.stringify(data), {
          expirationTtl: ttl
        });

        if (!isProvisional) {
          const truthId = key.replace("truth:", "");
          let index = [];
          try {
            const existingIndex = await env.memy.get("truths_index");
            if (existingIndex) index = JSON.parse(existingIndex);
          } catch (e) {
            console.warn("Index parse failed, rebuilding:", e);
            index = await this.rebuildIndexFromKeys(env);
          }
          if (!index.includes(truthId)) {
            index.push(truthId);
            await env.memy.put("truths_index", JSON.stringify(index));
          }
        }
      });
      return { success: true };
    } catch (error) {
      console.error("KV set error:", error);
      throw new Error("Failed to save truth");
    }
  },

  async checkVersionConflict(key, env, expectedVersion) {
    try {
      const existing = await this.getTruth(key, env);
      if (!existing) return false;
      return existing.version !== expectedVersion;
    } catch (error) {
      console.warn("Version check failed:", error);
      return false;
    }
  },

  async updateTruth(key, newData, env, expectedVersion = null) {
    try {
      await this.withRetry(async () => {
        if (
          expectedVersion &&
          (await this.checkVersionConflict(key, env, expectedVersion))
        ) {
          throw new Error("VERSION_CONFLICT");
        }

        newData.version = Date.now();
        newData.updated = Date.now();

        await env.memy.put(key, JSON.stringify(newData), {
          expirationTtl: 3600
        });

        if (!newData.provisional) {
          try {
            const truthId = key.replace("truth:", "");
            let index = await this.getIndex(env);
            if (!index.includes(truthId)) {
              index.push(truthId);
              await env.memy.put(
                "truths_index",
                JSON.stringify(index)
              );
            }
          } catch (indexError) {
            console.warn("Index update failed:", indexError);
          }
        }
      });

      return { success: true };
    } catch (error) {
      console.error("KV update error:", error);
      throw error;
    }
  },

  async getIndex(env) {
    try {
      const indexData = await env.memy.get("truths_index");
      return indexData ? JSON.parse(indexData) : [];
    } catch (error) {
      console.warn("Index access failed:", error);
      return [];
    }
  },

  async deleteTruth(key, env) {
    try {
      await this.withRetry(async () => {
        await env.memy.delete(key);
        await env.memy.delete(
          `truth:provisional:${key.replace("truth:", "")}`
        );

        try {
          const truthId = key.replace("truth:", "");
          let index = await this.getIndex(env);
          index = index.filter((id) => id !== truthId);
          await env.memy.put(
            "truths_index",
            JSON.stringify(index)
          );
        } catch (indexError) {
          console.warn("Index cleanup failed:", indexError);
        }
      });

      return { success: true };
    } catch (error) {
      console.error("KV delete error:", error);
      throw new Error("Failed to delete truth");
    }
  },

  async listTruths(env, limit = 50) {
    try {
      let truthIds = [];
      try {
        const index = await this.getIndex(env);
        if (Array.isArray(index) && index.length > 0) {
          truthIds = index.reverse();
        }
      } catch (e) {
        console.warn("Index access failed:", e);
      }

      if (truthIds.length === 0) {
        truthIds = await this.scanKVDirectly(env);
      }
      if (truthIds.length === 0) return [];

      const limitedIds = truthIds.slice(0, limit);
      const truths = [];

      for (const truthId of limitedIds) {
        try {
          const truthData = await this.getTruth(
            `truth:${truthId}`,
            env
          );
          if (truthData) {
            truths.push({
              id: truthId,
              query: truthData.query || truthId,
              answer: this.truncateAnswer(truthData.answer),
              created: truthData.created,
              updated: truthData.updated,
              edited: truthData.edited || false,
              editor: truthData.lastEditedBy,
              citations: truthData.citations || [],
              charCount: truthData.answer?.length || 0,
              model: truthData.model,
              version: truthData.version
            });
          }
        } catch (error) {
          console.warn(
            `Failed to load truth ${truthId}:`,
            error
          );
        }
      }

      return truths;
    } catch (error) {
      console.error("List truths error:", error);
      return [];
    }
  },

  async scanKVDirectly(env) {
    try {
      const allKeys = await env.memy.list({ prefix: "truth:" });
      return allKeys.keys
        .map((key) => key.name.replace("truth:", ""))
        .filter((id) => id && !id.startsWith("provisional:"));
    } catch (error) {
      console.error("Direct KV scan failed:", error);
      return [];
    }
  },

  async rebuildIndexFromKeys(env) {
    try {
      const allKeys = await env.memy.list({ prefix: "truth:" });
      return allKeys.keys
        .map((key) => key.name.replace("truth:", ""))
        .filter((id) => id && !id.startsWith("provisional:"));
    } catch (error) {
      console.error("Index rebuild failed:", error);
      return [];
    }
  },

  truncateAnswer(answer) {
    if (!answer) return "No answer";
    if (answer.length <= 200) return answer;
    return answer.substring(0, 200) + "...";
  }
};

// Helper for consistent truthId
function normalizeTruthId(idOrInput) {
  return KVHandler.normalizeQuery(idOrInput);
}

// ======================= MASTER TRUTHS (expert verified) =======================

const MasterTruthStore = {
  buildKey(inputOrId) {
    const id = normalizeTruthId(inputOrId);
    return `master:${id}`;
  },

  async get(env, inputOrId) {
    if (!env.MASTER_TRUTHS) return null;
    const key = this.buildKey(inputOrId);
    try {
      const raw = await env.MASTER_TRUTHS.get(key);
      return raw ? JSON.parse(raw) : null;
    } catch (err) {
      console.error("MasterTruth get error:", err);
      return null;
    }
  },

  async set(env, { idOrInput, answer, metadata = {}, editor = "expert", source = "manual" }) {
    if (!env.MASTER_TRUTHS) throw new Error("MASTER_TRUTHS not bound");
    const key = this.buildKey(idOrInput);
    const now = Date.now();

    const record = {
      id: key.replace(/^master:/, ""),
      query: idOrInput,
      answer,
      created: now,
      updated: now,
      editor,
      source,
      verified: true,
      version: now,
      metadata
    };

    await env.MASTER_TRUTHS.put(key, JSON.stringify(record));
    return record;
  },

  async update(env, { idOrInput, answer, editor = "expert", metadata = {}, expectedVersion }) {
    if (!env.MASTER_TRUTHS) throw new Error("MASTER_TRUTHS not bound");
    const key = this.buildKey(idOrInput);
    const raw = await env.MASTER_TRUTHS.get(key);
    if (!raw) {
      const e = new Error("NOT_FOUND");
      e.code = "NOT_FOUND";
      throw e;
    }

    const existing = JSON.parse(raw);
    if (expectedVersion && existing.version !== expectedVersion) {
      const e = new Error("VERSION_CONFLICT");
      e.code = "VERSION_CONFLICT";
      e.current = existing;
      throw e;
    }

    const now = Date.now();
    const updated = {
      ...existing,
      answer: answer ?? existing.answer,
      updated: now,
      editor,
      metadata: { ...existing.metadata, ...metadata },
      version: now
    };

    await env.MASTER_TRUTHS.put(key, JSON.stringify(updated));
    return updated;
  },

  async delete(env, idOrInput) {
    if (!env.MASTER_TRUTHS) throw new Error("MASTER_TRUTHS not bound");
    const key = this.buildKey(idOrInput);
    await env.MASTER_TRUTHS.delete(key);
    return { success: true };
  }
};

// ======================= DURABLE OBJECT: TruthMemory =======================

class TruthMemory extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
    this.sql = ctx.storage.sql;
    this._initPromise = null;
    this._inited = false;
  }

  async _ensureInit() {
    if (this._inited) return;
    if (this._initPromise) return this._initPromise;

    this._initPromise = (async () => {
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS session_preferences (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          preference_type TEXT NOT NULL,
          preference_value TEXT NOT NULL,
          confidence REAL DEFAULT 0.5,
          is_explicit BOOLEAN DEFAULT 0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS session_intents (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          intent_description TEXT NOT NULL,
          confidence REAL DEFAULT 0.5,
          intent_type TEXT DEFAULT 'general',
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS interactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          query TEXT NOT NULL,
          response TEXT NOT NULL,
          timestamp INTEGER NOT NULL,
          session_id TEXT NOT NULL,
          model TEXT DEFAULT 'llama-3-8b',
          summary TEXT,
          is_summarized BOOLEAN DEFAULT 0
        );
        
        CREATE TABLE IF NOT EXISTS session_state (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_id TEXT NOT NULL,
          state_key TEXT NOT NULL,
          state_value TEXT NOT NULL,
          updated_at INTEGER NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS idx_prefs_session ON session_preferences(session_id);
        CREATE INDEX IF NOT EXISTS idx_intents_session ON session_intents(session_id);
        CREATE INDEX IF NOT EXISTS idx_interactions_session ON interactions(session_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_state_session ON session_state(session_id, state_key);
      `);
      this._inited = true;
    })();

    return this._initPromise;
  }

  generateInteractionSummary(query, answer) {
    const qp = query.length > 100 ? query.slice(0, 100) + "..." : query;
    const ap = answer.length > 150 ? answer.slice(0, 150) + "..." : answer;
    return `User asked about "${qp}" and received explanation about "${ap}"`;
  }

  extractStateFacts(query, answer) {
    const facts = [];
    const text = `${query} ${answer}`.toLowerCase();

    const stageMatch = text.match(/(?:stage|phase)\s+(\d+)/);
    if (stageMatch) facts.push({ key: "audit_stage", value: stageMatch[1] });

    const modelMatch = text.match(/model\s+(?:is|are)?\s*(\w+)/);
    if (modelMatch) facts.push({ key: "current_model", value: modelMatch[1] });

    const versionMatch = text.match(/version\s+(\d+)/);
    if (versionMatch) facts.push({ key: "version", value: versionMatch[1] });

    return facts;
  }

  async addInteraction(query, answer, model = "llama-3-8b") {
    await this._ensureInit();
    const sessionId = this.ctx.id.toString();
    const timestamp = Date.now();
    const summary = this.generateInteractionSummary(query, answer);

    const result = this.sql.exec(
      `
      INSERT INTO interactions (query, response, timestamp, session_id, model, summary, is_summarized)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `,
      [query, answer, timestamp, sessionId, model, summary, 1]
    );

    await this.checkAndUpdateSummarization();
    await this.updateState(query, answer);

    this.sql.exec(
      `
      DELETE FROM interactions WHERE session_id = ? AND id NOT IN (
        SELECT id FROM interactions WHERE session_id = ? ORDER BY timestamp DESC LIMIT 50
      )
    `,
      [sessionId, sessionId]
    );

    return { success: true, id: result.lastInsertRowId };
  }

  async updateState(query, answer) {
    await this._ensureInit();
    const sessionId = this.ctx.id.toString();
    const timestamp = Date.now();
    const facts = this.extractStateFacts(query, answer);

    for (const fact of facts) {
      this.sql.exec(
        `
        INSERT OR REPLACE INTO session_state (session_id, state_key, state_value, updated_at)
        VALUES (?, ?, ?, ?)
      `,
        [sessionId, fact.key, fact.value, timestamp]
      );
    }
  }

  async getState(key = null) {
    await this._ensureInit();
    const sessionId = this.ctx.id.toString();
    let sql = `SELECT state_key, state_value FROM session_state WHERE session_id = ?`;
    const params = [sessionId];
    if (key) {
      sql += ` AND state_key = ?`;
      params.push(key);
    }
    const rows = this.sql.exec(sql, params).toArray();
    if (key) return rows[0]?.state_value || null;
    return Object.fromEntries(
      rows.map((r) => [r.state_key, r.state_value])
    );
  }

  async checkAndUpdateSummarization() {
    const sessionId = this.ctx.id.toString();
    const res = this.sql
      .exec(
        `
      SELECT COUNT(*) as total FROM interactions WHERE session_id = ?
    `,
        [sessionId]
      )
      .toArray();
    const total = res[0]?.total || 0;
    if (total >= 20 && total % 5 === 0) {
      await this.generateLongTermSummary();
    }
  }

  async generateLongTermSummary() {
    const sessionId = this.ctx.id.toString();
    const rows = this.sql
      .exec(
        `
      SELECT query, response, summary FROM interactions 
      WHERE session_id = ? AND id NOT IN (
        SELECT id FROM interactions WHERE session_id = ? ORDER BY timestamp DESC LIMIT 5
      )
      ORDER BY timestamp ASC
    `,
        [sessionId, sessionId]
      )
      .toArray();
    if (rows.length < 10) return;

    const topics = this.extractConversationThemes(rows);
    const prefs = this.extractPersistentPreferences(rows);
    const summary = `Conversation covered topics like ${topics.join(
      ", "
    )}. User consistently showed preferences for ${prefs.join(
      ", "
    )}. This represents a ${rows.length}-interaction technical discussion with maintained context.`;

    await this.ctx.storage.put("long_term_summary", summary);
  }

  extractConversationThemes(interactions) {
    const themes = new Set();
    for (const i of interactions) {
      const text = `${i.query} ${i.response}`.toLowerCase();
      if (text.includes("api") || text.includes("endpoint"))
        themes.add("API development");
      if (text.includes("auth") || text.includes("login"))
        themes.add("authentication");
      if (text.includes("database") || text.includes("sql"))
        themes.add("database concepts");
      if (
        text.includes("javascript") ||
        text.includes("typescript")
      )
        themes.add("JavaScript/TypeScript");
      if (text.includes("error") || text.includes("debug"))
        themes.add("problem-solving");
      if (
        text.includes("optimize") ||
        text.includes("performance")
      )
        themes.add("optimization");
    }
    return Array.from(themes).slice(0, 4);
  }

  extractPersistentPreferences(interactions) {
    const prefs = new Set();
    for (const i of interactions) {
      const text = `${i.query} ${i.response}`.toLowerCase();
      if (
        text.includes("detailed") ||
        text.includes("thorough")
      )
        prefs.add("detailed explanations");
      if (text.includes("simple") || text.includes("basic"))
        prefs.add("simple explanations");
      if (
        text.includes("example") || text.includes("code")
      )
        prefs.add("practical examples");
      if (text.includes("step") || text.includes("how to"))
        prefs.add("step-by-step guidance");
    }
    return Array.from(prefs).slice(0, 3);
  }

  async updatePreferences(query, response) {
    await this._ensureInit();
    const sessionId = this.ctx.id.toString();
    const timestamp = Date.now();
    const prefs = this.extractBasicPreferences(query, response);

    for (const pref of prefs) {
      this.sql.exec(
        `
        INSERT OR REPLACE INTO session_preferences (
          id, session_id, preference_type, preference_value, confidence, is_explicit, created_at, updated_at
        ) VALUES (
          COALESCE((SELECT id FROM session_preferences WHERE session_id = ? AND preference_type = ? AND preference_value = ?), 
                   (SELECT COALESCE(MAX(id) + 1, 1) FROM session_preferences)),
          ?, ?, ?, ?, ?, ?, ?
        )
      `,
        [
          sessionId,
          pref.type,
          pref.value,
          sessionId,
          pref.type,
          pref.value,
          pref.confidence,
          pref.isExplicit ? 1 : 0,
          timestamp,
          timestamp
        ]
      );
    }

    const intent = this.extractBasicIntent(query, response);
    if (intent) {
      this.sql.exec(
        `
        INSERT OR REPLACE INTO session_intents (
          id, session_id, intent_description, confidence, intent_type, created_at, updated_at
        ) VALUES (
          COALESCE((SELECT id FROM session_intents WHERE session_id = ? AND intent_description = ?), 
                   (SELECT COALESCE(MAX(id) + 1, 1) FROM session_intents)),
          ?, ?, ?, ?, ?, ?
        )
      `,
        [
          sessionId,
          intent.description,
          sessionId,
          intent.description,
          intent.confidence,
          intent.type,
          timestamp,
          timestamp
        ]
      );
    }
  }

  extractBasicPreferences(query, response) {
    const prefs = [];
    const text = `${query} ${response}`.toLowerCase();

    if (
      text.includes("i prefer") ||
      text.includes("i like") ||
      text.includes("i want")
    ) {
      prefs.push({
        type: "explicit_preference",
        value: "user preference (see full text)",
        confidence: 0.8,
        isExplicit: true
      });
    }

    if (
      text.includes("coding") ||
      text.includes("programming")
    ) {
      prefs.push({
        type: "domain",
        value: "programming",
        confidence: 0.7,
        isExplicit: false
      });
    }

    if (
      text.includes("design") ||
      text.includes("ui") ||
      text.includes("ux")
    ) {
      prefs.push({
        type: "domain",
        value: "design",
        confidence: 0.7,
        isExplicit: false
      });
    }

    if (
      text.includes("concise") ||
      text.includes("brief")
    ) {
      prefs.push({
        type: "communication_style",
        value: "concise",
        confidence: 0.6,
        isExplicit: false
      });
    }

    if (
      text.includes("detailed") ||
      text.includes("thorough")
    ) {
      prefs.push({
        type: "communication_style",
        value: "detailed",
        confidence: 0.6,
        isExplicit: false
      });
    }

    return prefs;
  }

  extractBasicIntent(query, response) {
    const text = `${query} ${response}`.toLowerCase();
    if (
      text.includes("how to") ||
      text.includes("tutorial")
    ) {
      return {
        description: "learning",
        confidence: 0.7,
        type: "educational"
      };
    }
    if (
      text.includes("help") ||
      text.includes("assist")
    ) {
      return {
        description: "assistance",
        confidence: 0.6,
        type: "support"
      };
    }
    if (
      text.includes("explain") ||
      text.includes("what is")
    ) {
      return {
        description: "explanation",
        confidence: 0.6,
        type: "informational"
      };
    }
    return { description: "general", confidence: 0.3, type: "general" };
  }

  async getSessionData() {
    try {
      await this._ensureInit();
      const sessionId = this.ctx.id.toString();

      const explicitPrefs = this.sql
        .exec(
          `
        SELECT preference_value FROM session_preferences 
        WHERE session_id = ? AND is_explicit = 1
      `,
          [sessionId]
        )
        .toArray()
        .map((r) => r.preference_value);

      const inferredPrefs = this.sql
        .exec(
          `
        SELECT preference_value FROM session_preferences 
        WHERE session_id = ? AND is_explicit = 0
      `,
          [sessionId]
        )
        .toArray()
        .map((r) => r.preference_value);

      const currentIntentRow = this.sql
        .exec(
          `
        SELECT intent_description FROM session_intents 
        WHERE session_id = ? ORDER BY confidence DESC LIMIT 1
      `,
          [sessionId]
        )
        .toArray()[0];

      const interactions = this.sql
        .exec(
          `
        SELECT * FROM interactions WHERE session_id = ? ORDER BY timestamp DESC LIMIT 30
      `,
          [sessionId]
        )
        .toArray();

      const longTermSummary =
        (await this.ctx.storage.get("long_term_summary")) ||
        null;

      const state = await this.getState();

      return {
        explicitPreferences: explicitPrefs,
        inferredPreferences: inferredPrefs,
        currentIntent:
          currentIntentRow?.intent_description || "general",
        interactions,
        longTermSummary,
        state
      };
    } catch (error) {
      console.error("Error getting session data:", error);
      return null;
    }
  }

  async getHistory() {
    await this._ensureInit();
    const sessionId = this.ctx.id.toString();
    const interactions = this.sql
      .exec(
        `
      SELECT * FROM interactions WHERE session_id = ? ORDER BY timestamp DESC LIMIT 30
    `,
        [sessionId]
      )
      .toArray();
    return interactions;
  }

  async clearHistory() {
    try {
      await this._ensureInit();
      const sessionId = this.ctx.id.toString();

      this.sql.exec(
        "DELETE FROM interactions WHERE session_id = ?",
        [sessionId]
      );
      this.sql.exec(
        "DELETE FROM session_preferences WHERE session_id = ?",
        [sessionId]
      );
      this.sql.exec(
        "DELETE FROM session_intents WHERE session_id = ?",
        [sessionId]
      );
      this.sql.exec(
        "DELETE FROM session_state WHERE session_id = ?",
        [sessionId]
      );
      await this.ctx.storage.delete("long_term_summary");

      return {
        success: true,
        message:
          "Session context and history cleared after 2 hours"
      };
    } catch (error) {
      console.error("Clear error:", error);
      return { error: "Clear failed" };
    }
  }

  async alarm() {
    await this.clearHistory();
  }

  // DO fetch router: exposes internal API for the Worker
  async fetch(request) {
    await this._ensureInit();
    const url = new URL(request.url);
    const path = url.pathname;

    if (path.endsWith("/getSessionData") && request.method === "GET") {
      const data = await this.getSessionData();
      return new Response(JSON.stringify(data), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (path.endsWith("/getHistory") && request.method === "GET") {
      const history = await this.getHistory();
      return new Response(JSON.stringify(history), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (path.endsWith("/addInteraction") && request.method === "POST") {
      const { query, answer, model } = await request.json();
      const res = await this.addInteraction(query, answer, model);
      return new Response(JSON.stringify(res), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (path.endsWith("/clear") && request.method === "POST") {
      const res = await this.clearHistory();
      return new Response(JSON.stringify(res), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (path.endsWith("/export") && request.method === "GET") {
      const data = await this.getSessionData();
      const history = await this.getHistory();
      const snapshot = {
        version: "3.0",
        exportedAt: new Date().toISOString(),
        data,
        history
      };
      return new Response(JSON.stringify(snapshot), {
        status: 200,
        headers: { "Content-Type": "application/json" }
      });
    }

    if (path.endsWith("/import") && request.method === "POST") {
      // Simple import: replace with provided history/state
      const snapshot = await request.json();
      const sessionId = this.ctx.id.toString();

      this.sql.exec(
        "DELETE FROM interactions WHERE session_id = ?",
        [sessionId]
      );
      this.sql.exec(
        "DELETE FROM session_preferences WHERE session_id = ?",
        [sessionId]
      );
      this.sql.exec(
        "DELETE FROM session_intents WHERE session_id = ?",
        [sessionId]
      );
      this.sql.exec(
        "DELETE FROM session_state WHERE session_id = ?",
        [sessionId]
      );
      await this.ctx.storage.delete("long_term_summary");

      if (snapshot.history && Array.isArray(snapshot.history)) {
        for (const h of snapshot.history) {
          if (h.query && h.answer) {
            await this.addInteraction(
              h.query,
              h.answer,
              h.model || "llama-3-8b"
            );
          }
        }
      }

      if (snapshot.data?.state) {
        const now = Date.now();
        for (const [k, v] of Object.entries(
          snapshot.data.state
        )) {
          this.sql.exec(
            `
            INSERT OR REPLACE INTO session_state (session_id, state_key, state_value, updated_at)
            VALUES (?, ?, ?, ?)
          `,
            [sessionId, k, String(v), now]
          );
        }
      }

      return new Response(
        JSON.stringify({
          ok: true,
          imported:
            snapshot.history?.length || 0
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" }
        }
      );
    }

    return new Response("Not found", { status: 404 });
  }
}

// ======================= SESSION HELPERS =======================

function getOrCreateSessionId(request) {
  const url = new URL(request.url);
  const fromQuery = url.searchParams.get("sessionId");
  const fromHeader = request.headers.get("x-session-id");
  if (fromQuery) return fromQuery;
  if (fromHeader) return fromHeader;
  return generateSessionId(request); // fallback: fingerprint-based
}

function getSessionStub(env, sessionId) {
  const id = env.HISTORY.idFromName(sessionId);
  return env.HISTORY.get(id);
}

function generateSessionId(request) {
  const headers = request.headers;
  const ua = headers.get("User-Agent") || "";
  const lang = headers.get("Accept-Language") || "";
  const ip = headers.get("CF-Connecting-IP") || "unknown";
  const sessionData = `${ip}:${ua}:${lang}`;
  let hash = 0;
  for (let i = 0; i < sessionData.length; i++) {
    const c = sessionData.charCodeAt(i);
    hash = ((hash << 5) - hash) + c;
    hash |= 0;
  }
  return `session_${Math.abs(hash).toString(36)}`;
}

// ======================= MAIN: TruthEngine =======================

class TruthEngine {
  async fetch(request, env) {
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: CORS_HEADERS
      });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === "/" || path === "/api/ai/truth") {
        return await this.handleTruthQuery(request, env);
      }
      if (path === "/api/ai/edit-truth") {
        return await this.handleEditTruth(request, env);
      }
      if (path === "/api/ai/create-truth") {
        return await this.handleCreateTruth(request, env);
      }
      if (path === "/api/ai/list-truths") {
        return await this.handleListTruths(request, env);
      }
      if (path === "/api/ai/delete") {
        return await this.handleDeleteTruth(request, env);
      }
      if (path === "/conversation/history") {
        return await this.handleConversationHistory(
          request,
          env
        );
      }
      if (path === "/api/session/sync") {
        if (request.method === "GET") {
          return await this.handleExportSession(
            request,
            env
          );
        }
        if (request.method === "POST") {
          return await this.handleImportSession(
            request,
            env
          );
        }
      }
      if (path === "/debug/rebuild-index") {
        return await this.handleRebuildIndex(request, env);
      }
      if (path === "/debug/kv-status") {
        return await this.handleKVStatus(request, env);
      }
      // Master truths admin endpoints
      if (path === "/api/ai/master/create") {
        return await this.handleMasterCreate(request, env);
      }
      if (path === "/api/ai/master/update") {
        return await this.handleMasterUpdate(request, env);
      }
      if (path === "/api/ai/master/delete") {
        return await this.handleMasterDelete(request, env);
      }
      if (path === "/api/ai/master/get") {
        return await this.handleMasterGet(request, env);
      }

      return new Response(
        JSON.stringify({ error: "Not found" }),
        { status: 404, headers: CORS_HEADERS }
      );
    } catch (error) {
      console.error("Error:", error);
      return new Response(
        JSON.stringify({ error: error.message }),
        { status: 500, headers: CORS_HEADERS }
      );
    }
  }

  // ---------- /api/ai/truth ----------

  async handleTruthQuery(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({ error: "Method not allowed" }),
        { status: 405, headers: CORS_HEADERS }
      );
    }

    const body = await request.json();
    const {
      input,
      model = "llama-3-8b",
      challengeResponse
    } = body;

    if (!input?.trim()) {
      return new Response(
        JSON.stringify({ error: "Input required" }),
        { status: 400, headers: CORS_HEADERS }
      );
    }
    if (input.length > 5000) {
      return new Response(
        JSON.stringify({
          error: "Query too long (max 5000 chars)"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    const modelConfig = MODELS[model];
    if (!modelConfig) {
      return new Response(
        JSON.stringify({ error: "Invalid model" }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    if (modelConfig.requiresChallenge) {
      const ok = await this.validateChallenge(
        challengeResponse,
        env
      );
      if (!ok) {
        return new Response(
          JSON.stringify({
            error: "CHALLENGE_FAILED",
            challengeQuestion:
              modelConfig.challengeQuestion
          }),
          { status: 403, headers: CORS_HEADERS }
        );
      }
    }

    const sessionId = getOrCreateSessionId(request);
    const stub = getSessionStub(env, sessionId);

    // Build session context (best-effort)
    let sessionContext = "";
    try {
      const res = await stub.fetch(
        "https://internal/session/getSessionData"
      );
      if (res.ok) {
        const data = await res.json();
        sessionContext =
          this.buildSessionContext(data);
      }
    } catch (e) {
      console.warn("Failed to get session context:", e);
    }

    // 1) Master truths first
    const master = await MasterTruthStore.get(env, input);
    if (master && master.answer) {
      stub.fetch("https://internal/session/addInteraction", {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          query: input,
          answer: master.answer,
          model: master.source || "master_truth"
        })
      });

      return new Response(
        JSON.stringify({
          answer: master.answer,
          model: master.source || "master_truth",
          verified: true,
          masterId: master.id,
          cached: true,
          source: "master_truth",
          created: master.created,
          updated: master.updated,
          sessionId
        }),
        { headers: CORS_HEADERS }
      );
    }

    // 2) KV caches (final/provisional) as in your logic
    const normalizedId = normalizeTruthId(input);
    const finalKey = `truth:${normalizedId}`;
    const provisionalKey = `truth:provisional:${normalizedId}`;

    const finalRecord = await KVHandler.getTruth(
      finalKey,
      env
    );
    if (finalRecord) {
      const provisionalRecord =
        await KVHandler.getTruth(
          provisionalKey,
          env
        );
      const chosen =
        provisionalRecord &&
        provisionalRecord.version >
          (finalRecord.version || 0)
          ? provisionalRecord
          : finalRecord;

      stub.fetch(
        "https://internal/session/addInteraction",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            query: input,
            answer: chosen.answer,
            model
          })
        }
      );

      return new Response(
        JSON.stringify({
          ...chosen,
          cached: true,
          source: chosen.edited
            ? "human_cache"
            : "provisional_cache",
          sessionId
        }),
        { headers: CORS_HEADERS }
      );
    }

    const provisionalCache =
      await KVHandler.getTruth(
        provisionalKey,
        env
      );
    if (
      provisionalCache &&
      !provisionalCache.edited
    ) {
      stub.fetch(
        "https://internal/session/addInteraction",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            query: input,
            answer: provisionalCache.answer,
            model
          })
        }
      );

      return new Response(
        JSON.stringify({
          ...provisionalCache,
          cached: true,
          source: "provisional_cache",
          sessionId
        }),
        { headers: CORS_HEADERS }
      );
    }

    // 3) AI generation
    try {
      const response = await this.generateResponse(
        env,
        modelConfig,
        input,
        sessionContext
      );

      const cacheData = {
        answer: response.answer,
        model,
        confidence: response.confidence || null,
        created: Date.now(),
        query: input,
        citations: response.citations || [],
        provisional: true,
        version: Date.now()
      };

      await KVHandler.setTruth(
        provisionalKey,
        cacheData,
        env,
        true
      );

      stub.fetch(
        "https://internal/session/addInteraction",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            query: input,
            answer: response.answer,
            model
          })
        }
      );
      stub.fetch(
        "https://internal/session/updatePreferences",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            query: input,
            response: response.answer
          })
        }
      ).catch(() => {});

      return new Response(
        JSON.stringify({
          ...cacheData,
          cached: false,
          source: "ai_generated",
          sessionId
        }),
        { headers: CORS_HEADERS }
      );
    } catch (error) {
      console.error(
        `AI generation failed for "${input}":`,
        error
      );
      return new Response(
        JSON.stringify({
          error: "AI generation failed",
          message: error.message
        }),
        { status: 500, headers: CORS_HEADERS }
      );
    }
  }

  // ---------- Session export/import (/api/session/sync) ----------

  async handleExportSession(request, env) {
    const sessionId = getOrCreateSessionId(request);
    const stub = getSessionStub(env, sessionId);

    const res = await stub.fetch(
      "https://internal/session/export"
    );
    if (!res.ok) {
      return new Response(
        JSON.stringify({ error: "Failed to export" }),
        { status: 500, headers: CORS_HEADERS }
      );
    }

    const snapshot = await res.json();

    const exportData = {
      version: "3.0",
      exportedAt: new Date().toISOString(),
      source: "truth-engine-server",
      conversations: (snapshot.history || []).map(
        (h) => ({
          id: `server_${h.id}`,
          query: h.query,
          normalizedQuery: normalizeTruthId(
            h.query
          ),
          answer: h.response,
          model: h.model,
          timestamp: h.timestamp,
          source: "server",
          wordCount: h.response.split(" ")
            .length,
          characterCount: h.response.length,
          summary: h.summary || null
        })
      ),
      currentModel:
        snapshot.data?.currentIntent ||
        "llama-3-8b",
      theme: "silvery",
      settings: {
        localHistoryCount:
          snapshot.history?.length || 0,
        hasRemoteHistory: true,
        explicitPreferences:
          snapshot.data
            ?.explicitPreferences || [],
        inferredPreferences:
          snapshot.data
            ?.inferredPreferences || [],
        state: snapshot.data?.state || {}
      }
    };

    return new Response(
      JSON.stringify(exportData, null, 2),
      {
        status: 200,
        headers: {
          ...CORS_HEADERS,
          "Content-Disposition":
            `attachment; filename="truth-session-${sessionId}.json"`
        }
      }
    );
  }

  async handleImportSession(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }

    const body = await request.json();
    const snapshot = body.snapshot || body;
    const { conversations, settings } = snapshot;

    if (
      !Array.isArray(conversations) ||
      conversations.length === 0
    ) {
      return new Response(
        JSON.stringify({
          error:
            "Invalid format: missing conversations"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    // New sessionId for imported context
    const sessionId =
      body.sessionId || crypto.randomUUID();
    const stub = getSessionStub(env, sessionId);

    const importPayload = {
      history: conversations.map((c) => ({
        query: c.query,
        answer: c.answer,
        model: c.model || "llama-3-8b",
        timestamp:
          c.timestamp || Date.now()
      })),
      data: {
        state: settings?.state || {}
      }
    };

    const res = await stub.fetch(
      "https://internal/session/import",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify(importPayload)
      }
    );

    if (!res.ok) {
      return new Response(
        JSON.stringify({
          error: "Failed to import snapshot"
        }),
        { status: 500, headers: CORS_HEADERS }
      );
    }

    const imported = await res.json();

    return new Response(
      JSON.stringify({
        ok: true,
        sessionId,
        imported: imported.imported
      }),
      { status: 200, headers: CORS_HEADERS }
    );
  }

  // ---------- CRUD for truths (unchanged core, using KVHandler) ----------

  async handleCreateTruth(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }
    const body = await request.json();
    const { truthId, input, newText, editor, citations } =
      body;

    if (!newText || (!truthId && !input)) {
      return new Response(
        JSON.stringify({
          error:
            "newText and truthId or input are required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    const normalizedId = normalizeTruthId(
      truthId || input
    );
    const finalKey = `truth:${normalizedId}`;
    const now = Date.now();
    const existing = await KVHandler.getTruth(
      finalKey,
      env
    );
    if (
      existing &&
      existing.version &&
      existing.version > (body.expectedVersion || 0)
    ) {
      return new Response(
        JSON.stringify({
          error: "VERSION_CONFLICT",
          message:
            "This truth was modified elsewhere",
          currentVersion: existing.version
        }),
        { status: 409, headers: CORS_HEADERS }
      );
    }

    const data = {
      query: input || normalizedId,
      answer: newText,
      edited: true,
      lastEditedBy: editor || "admin",
      citations: citations || [],
      created: existing?.created || now,
      updated: now,
      provisional: false,
      model: "manual",
      version: now
    };

    await KVHandler.updateTruth(
      finalKey,
      data,
      env
    );
    await KVHandler.deleteTruth(
      `truth:provisional:${normalizedId}`,
      env
    );

    return new Response(
      JSON.stringify({
        success: true,
        message:
          "Truth created successfully",
        id: normalizedId,
        version: now
      }),
      { headers: CORS_HEADERS }
    );
  }

  async handleEditTruth(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }
    const body = await request.json();
    const {
      truthId,
      input,
      newText,
      editor,
      citations,
      expectedVersion
    } = body;

    if (!newText || (!truthId && !input)) {
      return new Response(
        JSON.stringify({
          error:
            "newText and truthId or input are required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    const normalizedId = normalizeTruthId(
      truthId || input
    );
    const finalKey = `truth:${normalizedId}`;

    if (
      !(await KVHandler.getTruth(finalKey, env))
    ) {
      return await this.handleCreateTruth(
        request,
        env
      );
    }

    if (expectedVersion) {
      const conflict =
        await KVHandler.checkVersionConflict(
          finalKey,
          env,
          expectedVersion
        );
      if (conflict) {
        const current =
          await KVHandler.getTruth(
            finalKey,
            env
          );
        return new Response(
          JSON.stringify({
            error: "VERSION_CONFLICT",
            message:
              "This truth was modified elsewhere",
            currentVersion: current?.version
          }),
          { status: 409, headers: CORS_HEADERS }
        );
      }
    }

    const updatedData = {
      query: input || normalizedId,
      answer: newText,
      edited: true,
      lastEditedBy: editor || "admin",
      citations: citations || [],
      updated: Date.now(),
      provisional: false,
      model: "manual",
      version: Date.now()
    };

    await KVHandler.updateTruth(
      finalKey,
      updatedData,
      env
    );
    await KVHandler.deleteTruth(
      `truth:provisional:${normalizedId}`,
      env
    );

    return new Response(
      JSON.stringify({
        success: true,
        message:
          "Truth updated successfully",
        id: normalizedId,
        version: updatedData.version
      }),
      { headers: CORS_HEADERS }
    );
  }

  async handleDeleteTruth(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }

    const body = await request.json();
    const { truthId, input } = body;

    if (!truthId && !input) {
      return new Response(
        JSON.stringify({
          error:
            "truthId or input is required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    const normalizedId = normalizeTruthId(
      truthId || input
    );
    const finalKey = `truth:${normalizedId}`;

    await KVHandler.deleteTruth(
      finalKey,
      env
    );

    return new Response(
      JSON.stringify({
        success: true,
        message:
          "Truth deleted successfully",
        id: normalizedId
      }),
      { headers: CORS_HEADERS }
    );
  }

  async handleListTruths(request, env) {
    if (request.method !== "GET") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }

    try {
      const url = new URL(request.url);
      const limit = Math.min(
        parseInt(
          url.searchParams.get("limit") ||
            "50",
          10
        ),
        200
      );

      const truths =
        await KVHandler.listTruths(
          env,
          limit
        );

      return new Response(
        JSON.stringify({
          truths,
          total: truths.length
        }),
        { headers: CORS_HEADERS }
      );
    } catch (error) {
      console.error(
        "Error listing truths:",
        error
      );
      return new Response(
        JSON.stringify({
          error: error.message,
          truths: [],
          total: 0
        }),
        { status: 500, headers: CORS_HEADERS }
      );
    }
  }

  async handleConversationHistory(
    request,
    env
  ) {
    if (request.method !== "GET") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }

    try {
      const sessionId =
        getOrCreateSessionId(request);
      const stub = getSessionStub(env, sessionId);
      const res = await stub.fetch(
        "https://internal/session/getHistory"
      );
      const history = res.ok
        ? await res.json()
        : [];

      return new Response(
        JSON.stringify({
          history,
          sessionActive: true,
          expiresAt:
            "2 hours of inactivity"
        }),
        { headers: CORS_HEADERS }
      );
    } catch (error) {
      console.error(
        "Error getting conversation history:",
        error
      );
      return new Response(
        JSON.stringify({
          error: error.message,
          history: []
        }),
        { status: 500, headers: CORS_HEADERS }
      );
    }
  }

  async handleRebuildIndex(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }

    try {
      const truthIds =
        await KVHandler.rebuildIndexFromKeys(
          env
        );
      await env.memy.put(
        "truths_index",
        JSON.stringify(truthIds)
      );

      return new Response(
        JSON.stringify({
          success: true,
          message:
            `Index rebuilt with ${truthIds.length} truths`,
          truthCount: truthIds.length
        }),
        { headers: CORS_HEADERS }
      );
    } catch (error) {
      console.error(
        "Index rebuild failed:",
        error
      );
      return new Response(
        JSON.stringify({
          error: error.message
        }),
        {
          status: 500,
          headers: CORS_HEADERS
        }
      );
    }
  }

  async handleKVStatus(request, env) {
    try {
      const allKeys = await env.memy.list();
      const truthKeys = allKeys.keys.filter(
        (k) => k.name.startsWith("truth:")
      );
      const provisionalKeys =
        truthKeys.filter((k) =>
          k.name.includes("provisional:")
        );
      const indexKeys =
        allKeys.keys.filter((k) =>
          k.name.includes("index")
        );

      return new Response(
        JSON.stringify({
          totalKeys: allKeys.keys.length,
          truthKeysCount:
            truthKeys.length -
            provisionalKeys.length,
          provisionalKeysCount:
            provisionalKeys.length,
          truthKeys: truthKeys
            .slice(0, 20)
            .map((k) => k.name),
          indexExists: indexKeys.length > 0,
          sessionMemory:
            "Enabled (DO with 2-hour auto-cleanup)"
        }),
        { headers: CORS_HEADERS }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({
          error: error.message
        }),
        {
          status: 500,
          headers: CORS_HEADERS
        }
      );
    }
  }

  // ---------- Master truths admin handlers ----------

  async handleMasterCreate(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }
    const body = await request.json();
    const { input, answer, editor, metadata } =
      body;

    if (!input || !answer) {
      return new Response(
        JSON.stringify({
          error:
            "input and answer required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    const record = await MasterTruthStore.set(
      env,
      {
        idOrInput: input,
        answer,
        editor: editor || "expert",
        metadata: metadata || {}
      }
    );

    return new Response(
      JSON.stringify({
        success: true,
        record
      }),
      { headers: CORS_HEADERS }
    );
  }

  async handleMasterUpdate(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }
    const body = await request.json();
    const {
      input,
      answer,
      editor,
      metadata,
      expectedVersion
    } = body;

    if (!input || !answer) {
      return new Response(
        JSON.stringify({
          error:
            "input and answer required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    try {
      const record =
        await MasterTruthStore.update(
          env,
          {
            idOrInput: input,
            answer,
            editor: editor || "expert",
            metadata: metadata || {},
            expectedVersion
          }
        );
      return new Response(
        JSON.stringify({
          success: true,
          record
        }),
        { headers: CORS_HEADERS }
      );
    } catch (err) {
      if (err.code === "VERSION_CONFLICT") {
        return new Response(
          JSON.stringify({
            error: "VERSION_CONFLICT",
            current: err.current
          }),
          { status: 409, headers: CORS_HEADERS }
        );
      }
      if (err.code === "NOT_FOUND") {
        return new Response(
          JSON.stringify({
            error: "Not found"
          }),
          { status: 404, headers: CORS_HEADERS }
        );
      }
      throw err;
    }
  }

  async handleMasterDelete(request, env) {
    if (request.method !== "POST") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }
    const body = await request.json();
    const { input } = body;
    if (!input) {
      return new Response(
        JSON.stringify({
          error:
            "input required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    await MasterTruthStore.delete(env, input);
    return new Response(
      JSON.stringify({
        success: true
      }),
      { headers: CORS_HEADERS }
    );
  }

  async handleMasterGet(request, env) {
    if (request.method !== "GET") {
      return new Response(
        JSON.stringify({
          error: "Method not allowed"
        }),
        { status: 405, headers: CORS_HEADERS }
      );
    }
    const url = new URL(request.url);
    const id =
      url.searchParams.get("id") ||
      url.searchParams.get("q");
    if (!id) {
      return new Response(
        JSON.stringify({
          error:
            "id or q required"
        }),
        { status: 400, headers: CORS_HEADERS }
      );
    }

    const record = await MasterTruthStore.get(
      env,
      id
    );
    if (!record) {
      return new Response(
        JSON.stringify({
          error: "Not found"
        }),
        { status: 404, headers: CORS_HEADERS }
      );
    }
    return new Response(
      JSON.stringify(record),
      { headers: CORS_HEADERS }
    );
  }

  // ---------- Context building & utilities ----------

  buildSessionContext(sessionData) {
    if (!sessionData) return "";
    let context = "";

    if (
      sessionData.explicitPreferences
      && sessionData.explicitPreferences.length
    ) {
      context +=
        `User's stated preferences: ${sessionData.explicitPreferences.join(
          ", "
        )}\n\n`;
    }

    if (
      sessionData.inferredPreferences
      && sessionData.inferredPreferences.length
    ) {
      context +=
        `Inferred preferences: ${sessionData.inferredPreferences.join(
          ", "
        )}\n\n`;
    }

    if (sessionData.currentIntent) {
      context +=
        `Current intent: ${sessionData.currentIntent}\n\n`;
    }

    if (
      sessionData.state &&
      Object.keys(sessionData.state).length
    ) {
      context +=
        "Session state (remember these facts):\n";
      for (const [k, v] of Object.entries(
        sessionData.state
      )) {
        context +=
          `${k.replace(/_/g, " ")}: ${v}\n`;
      }
      context += "\n";
    }

    const xs = sessionData.interactions || [];
    if (xs.length) {
      context +=
        "Conversation history (enhanced memory):\n";
      const recent = xs.slice(-5);
      if (recent.length) {
        context +=
          "Recent detailed conversation:\n";
        context += recent
          .map(
            (h) =>
              `User: ${h.query}\nAI: ${h.response}`
          )
          .join("\n\n");
        context += "\n\n";
      }

      const medium = xs.slice(-15, -5);
      if (medium.length) {
        context +=
          "Recent conversation summaries:\n";
        context += medium
          .map((h) =>
            h.summary
              ? `Summary: ${h.summary}`
              : `User asked about "${h.query?.substring(
                  0,
                  50
                )}..."`
          )
          .join("\n");
        context += "\n\n";
      }

      if (xs.length > 15) {
        context +=
          "Long-term conversation themes:\n";
        context +=
          `${sessionData.longTermSummary || "Ongoing technical discussion with consistent user preferences."}\n\n`;
      }
    }

    return context;
  }

  async validateChallenge(response, env) {
    if (!response) return false;
    const keywords = (env.MODEL_PASSPHRASE ||
      "truth engine")
      .toLowerCase()
      .split(" ");
    const lower = response.toLowerCase();
    const hits = keywords.filter((k) =>
      lower.includes(k)
    ).length;
    return hits >= 2;
  }

  async generateResponse(env, modelConfig, input, context) {
    const systemPrompt =
      env.AI_SYSTEM_PROMPT ||
      "You are a helpful AI assistant.";
    const fullPrompt = `${systemPrompt}

${context}

USER: ${input}

Respond clearly and helpfully:`;

    const resp = await fetch(
      "https://openrouter.ai/api/v1/chat/completions",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.OPENROUTER_API_KEY}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          model: modelConfig.model,
          messages: [
            { role: "user", content: fullPrompt }
          ]
        })
      }
    );

    if (!resp.ok) {
      throw new Error(
        `OpenRouter error: ${await resp.text()}`
      );
    }

    const data = await resp.json();
    return { answer: data.choices[0].message.content };
  }
}

// ======================= EXPORTS =======================

export { TruthMemory };

export default {
  fetch: (req, env) => new TruthEngine().fetch(req, env)
};