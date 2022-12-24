#include <assert.h>
#include <readosm.h>
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>

struct InsertNodeContext {
  sqlite3 *dbHandle;
  sqlite3_stmt *insertNodeStmt;
  sqlite3_stmt *insertTagStmt;
};

struct InsertWayContext {
  sqlite3 *dbHandle;
  sqlite3_stmt *insertWayStmt;
  sqlite3_stmt *insertTagStmt;
  sqlite3_stmt *insertNodeRefStmt;
};

struct OsmParseContext {
  int nodes;
  int ways;
  int relation;

  struct InsertNodeContext insertNodeContext;
  struct InsertWayContext insertWayContext;
};

static int prepareInsertNodeStatement(struct InsertNodeContext *ctx) {
  static const char *nodeQuery = "INSERT OR IGNORE INTO nodes "
                                 "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);";
  int ret;
  const char *tail;
  sqlite3 *handle = ctx->dbHandle;

  return sqlite3_prepare_v2(handle, nodeQuery, -1, &ctx->insertNodeStmt, &tail);
}

static int prepareInsertWayStatement(struct InsertWayContext *ctx) {
  static const char *nodeQuery = "INSERT OR IGNORE INTO ways "
                                 "VALUES (?1, ?2, ?3, ?4, ?5);";
  int ret;
  const char *tail;
  sqlite3 *handle = ctx->dbHandle;

  return sqlite3_prepare_v2(handle, nodeQuery, -1, &ctx->insertWayStmt, &tail);
}

static int prepareInsertNodeTagStatement(struct InsertNodeContext *ctx) {
  static const char *tagQuery =
      "INSERT OR IGNORE INTO node_tags(node_id, key, value) "
      "VALUES (?1, ?2, ?3);";
  const char *tail;
  sqlite3 *handle = ctx->dbHandle;
  return sqlite3_prepare_v2(handle, tagQuery, -1, &ctx->insertTagStmt, &tail);
}

static int prepareInsertWayTagStatement(struct InsertWayContext *ctx) {
  static const char *tagQuery =
      "INSERT OR IGNORE INTO way_tags(way_id, key, value) "
      "VALUES (?1, ?2, ?3);";
  const char *tail;
  sqlite3 *handle = ctx->dbHandle;
  return sqlite3_prepare_v2(handle, tagQuery, -1, &ctx->insertTagStmt, &tail);
}

static int
prepareInsertWayNodeReferenceStatement(struct InsertWayContext *ctx) {
  static const char *tagQuery =
      "INSERT OR IGNORE INTO way_nodes(way_id, node_id) "
      "VALUES (?1, ?2);";
  const char *tail;
  sqlite3 *handle = ctx->dbHandle;
  return sqlite3_prepare_v2(handle, tagQuery, -1, &ctx->insertNodeRefStmt,
                            &tail);
}

static int initInsertNodeContext(sqlite3 *db, struct InsertNodeContext *ctx) {
  ctx->dbHandle = db;
  ctx->insertNodeStmt = NULL;
  ctx->insertTagStmt = NULL;
  int ret;
  if ((ret = prepareInsertNodeStatement(ctx)) != SQLITE_OK) {
    fprintf(stderr, "Failed to prepare insert node statement: %d\n", ret);
    return ret;
  }

  if ((ret = prepareInsertNodeTagStatement(ctx)) != SQLITE_OK) {
    fprintf(stderr, "Failed to prepare insert node tag statement: %d\n", ret);
    return ret;
  }

  return SQLITE_OK;
}

static int initInsertWayContext(sqlite3 *db, struct InsertWayContext *ctx) {
  ctx->dbHandle = db;
  ctx->insertWayStmt = NULL;
  ctx->insertTagStmt = NULL;
  ctx->insertNodeRefStmt = NULL;

  int ret;
  if ((ret = prepareInsertWayStatement(ctx)) != SQLITE_OK) {
    fprintf(stderr, "Failed to prepare insert way statement: %d\n", ret);
    return ret;
  }

  if ((ret = prepareInsertWayTagStatement(ctx)) != SQLITE_OK) {
    fprintf(stderr, "Failed to prepare insert way tag statement: %d\n", ret);
    return ret;
  }

  if ((ret = prepareInsertWayNodeReferenceStatement(ctx)) != SQLITE_OK) {
    fprintf(stderr,
            "Failed to prepare insert way node reference statement: %d\n", ret);
    return ret;
  }

  return SQLITE_OK;
}

static int insertNode(struct InsertNodeContext *ctx, const readosm_node *node);
static int insertWay(struct InsertWayContext *ctx, const readosm_way *way);
static int needPrint(int value) { return value != 0 && value % 100000 == 0; }

static void printStats(struct OsmParseContext *stats) {
  fprintf(stdout, "Nodes=%-10d Ways=%-10d Relation=%-10d\n", stats->nodes,
          stats->ways, stats->relation);
}

static void maybePrintStats(struct OsmParseContext *stats) {
  if (needPrint(stats->nodes) || needPrint(stats->relation) ||
      needPrint(stats->ways)) {
    printStats(stats);
  }
}

static int on_node(const void *user_data, const readosm_node *node) {
  struct OsmParseContext *stats = (struct OsmParseContext *)user_data;
  stats->nodes++;
  maybePrintStats(stats);
  int ret = insertNode(&stats->insertNodeContext, node);
  if (ret != SQLITE_OK) {
    fprintf(stderr, "Failed to insert node: %d\n", ret);
    return READOSM_ABORT;
  }

  return READOSM_OK;
}

static int on_way(const void *user_data, const readosm_way *way) {
  struct OsmParseContext *stats = (struct OsmParseContext *)user_data;
  stats->ways++;
  maybePrintStats(stats);
  int ret = insertWay(&stats->insertWayContext, way);
  if (ret != SQLITE_OK) {
    fprintf(stderr, "Failed to insert way: %d\n", ret);
    return READOSM_ABORT;
  }
  return READOSM_OK;
}

static int on_relation(const void *user_data,
                       const readosm_relation *relation) {
  ((struct OsmParseContext *)user_data)->relation++;
  maybePrintStats((struct OsmParseContext *)user_data);
  return READOSM_OK;
}

static int bindNode(sqlite3_stmt *stmt, const readosm_node *node) {
  int ret;
  if ((ret = sqlite3_bind_int64(stmt, 1, node->id)) != SQLITE_OK) {
    fprintf(stderr, "bindNode: Failed to bind 1 param to node statement");
    return ret;
  }

  if ((ret = sqlite3_bind_double(stmt, 2, node->latitude)) != SQLITE_OK) {
    fprintf(stderr, "bindNode: Failed to bind 2 param to node statement");
    return ret;
  }

  if ((ret = sqlite3_bind_double(stmt, 3, node->longitude)) != SQLITE_OK) {
    fprintf(stderr, "bindNode: Failed to bind 3 param to node statement");
    return ret;
  }

  if ((ret = sqlite3_bind_int64(stmt, 4, node->version)) != SQLITE_OK) {
    fprintf(stderr, "bindNode: Failed to bind 4 param to node statement");
    return ret;
  }

  if ((ret = sqlite3_bind_int64(stmt, 5, node->changeset)) != SQLITE_OK) {
    fprintf(stderr, "bindNode: Failed to bind 5 param to node statement");
    return ret;
  }

  if (node->user) {
    if ((ret = sqlite3_bind_text(stmt, 6, node->user, strlen(node->user),
                                 NULL)) != SQLITE_OK) {
      fprintf(stderr, "bindNode: Failed to bind 6 param to node statement");
      return ret;
    }
  } else {
    if ((ret = sqlite3_bind_text(stmt, 6, NULL, 0, NULL)) != SQLITE_OK) {
      fprintf(stderr,
              "bindNode: Failed to bind 6 (NULL) param to node statement");
      return ret;
    }
  }

  if ((ret = sqlite3_bind_int64(stmt, 7, node->uid)) != SQLITE_OK) {
    fprintf(stderr, "bindNode: Failed to bind 7 param to node statement");
    return ret;
  }

  if (node->timestamp) {
    if ((ret = sqlite3_bind_text(stmt, 8, node->timestamp,
                                 strlen(node->timestamp), NULL)) != SQLITE_OK) {
      fprintf(stderr, "bindNode: Failed to bind 8 param to node statement");
      return ret;
    }
  } else {
    if ((ret = sqlite3_bind_text(stmt, 8, NULL, 0, NULL)) != SQLITE_OK) {
      fprintf(stderr,
              "bindNode: Failed to bind 8 (NULL) param to node statement");
      return ret;
    }
  }

  return ret;
}

static int bindTag(sqlite3_stmt *stmt, long long parent_id,
                   const readosm_tag *tag) {
  int ret;
  if ((ret = sqlite3_bind_int64(stmt, 1, parent_id)) != SQLITE_OK) {
    fprintf(stderr, "bindTag: Failed to bind 1 param in tag statement");
    return ret;
  }

  if ((ret = sqlite3_bind_text(stmt, 2, tag->key, strlen(tag->key), NULL)) !=
      SQLITE_OK) {
    fprintf(stderr, "bindTag: Failed to bind 2 param in tag statement");
    return ret;
  }

  if ((ret = sqlite3_bind_text(stmt, 3, tag->value, strlen(tag->value),
                               NULL)) != SQLITE_OK) {
    fprintf(stderr, "bindTag: Failed to bind 3 param in tag statement");
    return ret;
  }

  return SQLITE_OK;
}

static int step(sqlite3_stmt *stmt) {
  int ret;
  if ((ret = sqlite3_step(stmt)) != SQLITE_DONE) {
    fprintf(stderr, "step: Failed to step node statement");
    return ret;
  }

  if ((ret = sqlite3_clear_bindings(stmt)) != SQLITE_OK) {
    fprintf(stderr, "step: Failed to clear bindings in node statement");
    return ret;
  }

  if ((ret = sqlite3_reset(stmt)) != SQLITE_OK) {
    fprintf(stderr, "step: Failed to reset node statement");
    return ret;
  }

  return SQLITE_OK;
}

static int insertNode(struct InsertNodeContext *ctx, const readosm_node *node) {

  int ret;
  const char *tail;
  char *errMsg;
  sqlite3 *handle = ctx->dbHandle;

  if ((ret = bindNode(ctx->insertNodeStmt, node)) != SQLITE_OK) {
    errMsg = "Failed to bind node";
    goto Fail;
  }

  if ((ret = step(ctx->insertNodeStmt)) != SQLITE_OK) {
    errMsg = "Failed to step node statement";
    goto Fail;
  }

  if (node->tag_count != 0) {
    for (int i = 0; i < node->tag_count; ++i) {
      if ((ret = bindTag(ctx->insertTagStmt, node->id, &node->tags[i])) !=
          SQLITE_OK) {
        errMsg = "insert_node: Failed to bind tag";
        goto Fail;
      }

      if ((ret = step(ctx->insertTagStmt)) != SQLITE_OK) {
        errMsg = "Failed to step node tag statement";
        goto Fail;
      }
    }
  }

  return SQLITE_OK;

Fail:
  fprintf(stderr, "%s\n", errMsg);
  fprintf(stderr, "%s\n", sqlite3_errmsg(handle));
  sqlite3_exec(handle, "END TRANSACTION", NULL, NULL, &errMsg);
  return ret;
}

static int bindWay(sqlite3_stmt *stmt, const readosm_way *way) {
  int ret;

  if ((ret = sqlite3_bind_int64(stmt, 1, way->id)) != SQLITE_OK) {
    fprintf(stderr, "bindWay: Failed to bind 1 param to node statement");
    return ret;
  }

  if ((ret = sqlite3_bind_int64(stmt, 2, way->changeset)) != SQLITE_OK) {
    fprintf(stderr, "bindWay: Failed to bind 2 param to node statement");
    return ret;
  }

  if (way->user) {
    if ((ret = sqlite3_bind_text(stmt, 3, way->user, strlen(way->user),
                                 NULL)) != SQLITE_OK) {
      fprintf(stderr, "bindWay: Failed to bind 3 param to node statement");
      return ret;
    }
  } else {
    if ((ret = sqlite3_bind_text(stmt, 3, NULL, 0, NULL)) != SQLITE_OK) {
      fprintf(stderr,
              "bindWay: Failed to bind 3 (NULL) param to node statement");
      return ret;
    }
  }

  if ((ret = sqlite3_bind_int64(stmt, 4, way->uid)) != SQLITE_OK) {
    fprintf(stderr, "bindWay: Failed to bind 4 param to node statement");
    return ret;
  }

  if (way->timestamp) {
    if ((ret = sqlite3_bind_text(stmt, 5, way->timestamp,
                                 strlen(way->timestamp), NULL)) != SQLITE_OK) {
      fprintf(stderr, "bindWay: Failed to bind 5 param to node statement");
      return ret;
    }
  } else {
    if ((ret = sqlite3_bind_text(stmt, 5, NULL, 0, NULL)) != SQLITE_OK) {
      fprintf(stderr,
              "bindWay: Failed to bind 5 (NULL) param to node statement");
      return ret;
    }
  }

  return ret;
}

static int bindNodeRef(sqlite3_stmt *stmt, long long parent, long long node) {
  int ret;

  if ((ret = sqlite3_bind_int64(stmt, 1, parent)) != SQLITE_OK) {
    fprintf(stderr, "bindNodeRef: Failed to bind 1 param to node statement");
    return ret;
  }

  if ((ret = sqlite3_bind_int64(stmt, 2, node)) != SQLITE_OK) {
    fprintf(stderr, "bindNodeRef: Failed to bind 2 param to node statement");
    return ret;
  }

  return ret;
}

static int insertWay(struct InsertWayContext *ctx, const readosm_way *way) {
  int ret;
  const char *tail;
  char *errMsg;
  sqlite3 *handle = ctx->dbHandle;

  if ((ret = bindWay(ctx->insertWayStmt, way)) != SQLITE_OK) {
    errMsg = "insertWay: Failed to bind way";
    goto Fail;
  }

  if ((ret = step(ctx->insertWayStmt)) != SQLITE_OK) {
    errMsg = "Failed to step insert way statement";
    goto Fail;
  }

  for (int i = 0; i < way->tag_count; ++i) {
    if ((ret = bindTag(ctx->insertTagStmt, way->id, &way->tags[i])) !=
        SQLITE_OK) {
      errMsg = "Failed to bind way tag";
      goto Fail;
    }

    if ((ret = step(ctx->insertTagStmt)) != SQLITE_OK) {
      errMsg = "Failed to step node way tag statement";
      goto Fail;
    }
  }

  for (int i = 0; i < way->node_ref_count; ++i) {
    if ((ret = bindNodeRef(ctx->insertNodeRefStmt, way->id,
                           way->node_refs[i])) != SQLITE_OK) {
      errMsg = "Failed to bind way node ref";
      goto Fail;
    }

    if ((ret = step(ctx->insertNodeRefStmt)) != SQLITE_OK) {
      errMsg = "Failed to step way node ref statement";
      goto Fail;
    }
  }

  return SQLITE_OK;

Fail:
  fprintf(stderr, "%s\n", errMsg);
  fprintf(stderr, "%s\n", sqlite3_errmsg(handle));
  sqlite3_exec(handle, "END TRANSACTION", NULL, NULL, &errMsg);
  return ret;
}

static int createTables(sqlite3 *handle) {

  const char *tableQueries[] = {
      "CREATE TABLE IF NOT EXISTS nodes ("
      "        id        INTEGER PRIMARY KEY,"
      "        latitude  REAL,"
      "        longitude REAL,"
      "        version   INTEGER,"
      "        changeset INTEGER,"
      "        user      TEXT,"
      "        uid       INTEGER,"
      "        timestamp TEXT"
      ");",
      "CREATE INDEX IF NOT EXISTS index_node_id ON nodes(id);",
      "CREATE TABLE IF NOT EXISTS node_tags ("
      "       node_id  INTEGER,"
      "       key      TEXT,"
      "       value    TEXT,"
      "       FOREIGN KEY (node_id) REFERENCES nodes(id)"
      ");",
      "CREATE INDEX IF NOT EXISTS index_node_tags_id ON node_tags(node_id);",
      "CREATE INDEX IF NOT EXISTS index_node_tags_key ON node_tags(key);",
      "CREATE TABLE IF NOT EXISTS ways ("
      "       id        INTEGER PRIMARY KEY,"
      "       changeset INTEGER,"
      "       user      TEXT,"
      "       uid       INTEGER,"
      "       timestamp TEXT"
      ");",
      "CREATE INDEX IF NOT EXISTS index_way_id ON ways(id);",
      "CREATE TABLE IF NOT EXISTS way_tags ("
      "       way_id    INTEGER,"
      "       key       TEXT,"
      "       value     TEXT,"
      "       FOREIGN KEY (way_id) REFERENCES ways(id)"
      ");",
      "CREATE INDEX IF NOT EXISTS index_way_tags_id ON way_tags(way_id);",
      "CREATE INDEX IF NOT EXISTS index_way_tags_key ON way_tags(key);",
      "CREATE TABLE IF NOT EXISTS way_nodes ("
      "       way_id    INTEGER,"
      "       node_id   INTEGER,"
      "       FOREIGN KEY (node_id) REFERENCES nodes(id),"
      "       FOREIGN KEY (way_id) REFERENCES ways(id)"
      ");",
      "CREATE INDEX IF NOT EXISTS index_way_nodes_way_id ON way_nodes(way_id);",
      "CREATE INDEX IF NOT EXISTS index_way_nodes_node_id ON "
      "way_nodes(node_id);",
      "CREATE TABLE IF NOT EXISTS node_names ("
      "       node_id   INTEGER,"
      "       name      TEXT,"
      "       FOREIGN KEY (node_id) REFERENCES nodes(id)"
      ");",

      "CREATE VIRTUAL TABLE named_nodes_fts5 USING fts5(id, name);",
      "CREATE VIRTUAL TABLE named_nodes_spellfix USING spellfix1;",
      "CREATE TRIGGER IF NOT EXISTS node_names AFTER INSERT ON node_tags "
      "WHEN new.key LIKE 'name%'"
      "BEGIN"
      "   INSERT INTO"
      "       node_names(node_id, name)"
      "   VALUES"
      "       (new.node_id, new.value);"
      ""
      "   INSERT INTO"
      "       named_nodes_fts5(id, name)"
      "   VALUES"
      "       (new.node_id, new.value);"
      "END;",

  };

  char *errMsg = NULL;
  int ret;

  for (int i = 0; i < sizeof(tableQueries) / sizeof(const char *); ++i) {
    const char *tableQuery = tableQueries[i];
    ret = sqlite3_exec(handle, tableQuery, NULL, NULL, &errMsg);
    if (ret != SQLITE_OK) {
      fprintf(stderr, "sqlite3_exec error: %s, running query\"%s\"", errMsg,
              tableQuery);
      sqlite3_free(errMsg);
      return ret;
    }
  }

  return SQLITE_OK;
}

int sqlite3_spellfix_init(sqlite3 *db, char **pzErrMsg,
                          const sqlite3_api_routines *pApi);

int main(int argc, char **argv) {
  assert(argc == 3);

  int ret = 0;
  const char *errMsg;
  sqlite3 *dbHandle = NULL;
  const void *osmHandle = NULL;

  if ((ret = sqlite3_auto_extension((void (*)(void)) &
                                    sqlite3_spellfix_init)) != SQLITE_OK) {
    errMsg = sqlite3_errstr(ret);
    goto Fail;
  }

  if ((ret = sqlite3_open(argv[2], &dbHandle)) != SQLITE_OK) {
    errMsg = sqlite3_errmsg(dbHandle);
    goto Fail;
  }

  if ((ret = createTables(dbHandle)) != SQLITE_OK) {
    errMsg = sqlite3_errmsg(dbHandle);
    goto Fail;
  }

  if ((ret = readosm_open(argv[1], &osmHandle)) != READOSM_OK) {
    errMsg = "Fail to open OSM";
    goto Fail;
  }

  struct OsmParseContext stats;
  memset(&stats, 0, sizeof(stats));
  stats.insertNodeContext.dbHandle = dbHandle;

  if ((ret = initInsertNodeContext(dbHandle, &stats.insertNodeContext)) !=
      SQLITE_OK) {
    errMsg = sqlite3_errmsg(dbHandle);
    goto Fail;
  }

  if ((ret = initInsertWayContext(dbHandle, &stats.insertWayContext)) !=
      SQLITE_OK) {
    errMsg = sqlite3_errmsg(dbHandle);
    goto Fail;
  }

  if ((ret = sqlite3_exec(dbHandle, "BEGIN TRANSACTION", NULL, NULL, NULL)) !=
      SQLITE_OK) {
    errMsg = sqlite3_errmsg(dbHandle);
    goto Fail;
  }

  if ((ret = readosm_parse(osmHandle, &stats, on_node, on_way, on_relation)) !=
      READOSM_OK) {
    errMsg = "Fail to parse OSM";
    goto Fail;
  }

  if ((ret = sqlite3_exec(dbHandle, "END TRANSACTION", NULL, NULL, NULL)) !=
      SQLITE_OK) {
    errMsg = sqlite3_errmsg(dbHandle);
    goto Fail;
  }

  if (stats.insertNodeContext.insertTagStmt != NULL) {
    ret = sqlite3_finalize(stats.insertNodeContext.insertTagStmt);
    if (ret != SQLITE_OK) {
      errMsg = "Failed to finalize tag statement";
      goto Fail;
    }
  }

  if (stats.insertNodeContext.insertNodeStmt != NULL) {
    ret = sqlite3_finalize(stats.insertNodeContext.insertNodeStmt);
    if (ret != SQLITE_OK) {
      errMsg = "insert_node: Failed to finalize node statement";
      goto Fail;
    }
  }

  sqlite3_close(dbHandle);
  readosm_close(osmHandle);
  printStats(&stats);
  return 0;

Fail:
  fprintf(stderr, "%s\n", errMsg);
  sqlite3_close(dbHandle);
  readosm_close(osmHandle);
  return ret;
}