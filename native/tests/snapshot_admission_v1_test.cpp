#include "ores/durable/snapshot_admission_v1.hpp"
#include <sqlite3.h>
#include <cassert>
#include <iostream>
#include <string>
using namespace ores::durable::snapshot_admission_v1;
static void sql(sqlite3* db,const char* text){assert(sqlite3_exec(db,text,nullptr,nullptr,nullptr)==SQLITE_OK);}
static bool read(sqlite3* db,const char* id){
    sqlite3_stmt* p=nullptr;
    assert(sqlite3_prepare_v2(db,query,-1,&p,nullptr)==SQLITE_OK);
    assert(sqlite3_bind_text(p,1,"t",-1,SQLITE_STATIC)==SQLITE_OK);
    assert(sqlite3_bind_text(p,2,"s",-1,SQLITE_STATIC)==SQLITE_OK);
    assert(sqlite3_bind_text(p,3,"Counter",-1,SQLITE_STATIC)==SQLITE_OK);
    assert(sqlite3_bind_text(p,4,id,-1,SQLITE_TRANSIENT)==SQLITE_OK);
    assert(sqlite3_step(p)==SQLITE_ROW);
    auto str=[&](int n){const auto size=sqlite3_column_bytes(p,n);return size?std::string_view(static_cast<const char*>(sqlite3_column_blob(p,n)),static_cast<std::size_t>(size)):std::string_view{};};
    bool valid=true;
    try{validate(sqlite3_column_int64(p,0),sqlite3_column_int64(p,1),str(2),str(3),sqlite3_column_int64(p,4),sqlite3_column_int(p,5)!=0);}catch(const std::logic_error&){valid=false;}
    assert(sqlite3_finalize(p)==SQLITE_OK);return valid;
}
int main(){
    sqlite3* db=nullptr;assert(sqlite3_open(":memory:",&db)==SQLITE_OK);
    sql(db,"CREATE TABLE sync_objects(tenant TEXT,space TEXT,actor_type TEXT,id TEXT,epoch INTEGER,revision INTEGER,schema_fp TEXT,snapshot BLOB,pruned_through INTEGER,PRIMARY KEY(tenant,space,actor_type,id));"
        "CREATE TABLE sync_operations(tenant TEXT,space TEXT,actor_type TEXT,id TEXT,op_id TEXT,PRIMARY KEY(tenant,space,actor_type,id,op_id));"
        "CREATE TABLE sync_outbox(tenant TEXT,space TEXT,actor_type TEXT,id TEXT,revision INTEGER,PRIMARY KEY(tenant,space,actor_type,id,revision));"
        "INSERT INTO sync_objects VALUES('t','s','Counter','a',1,0,'',X'',0),('t','s','Counter','b',2,0,'',X'',0);");
    assert(read(db,"a")&&read(db,"b"));
    sql(db,"INSERT INTO sync_operations VALUES('t','s','Counter','a','old');");assert(!read(db,"a")&&read(db,"b"));
    sql(db,"DELETE FROM sync_operations; INSERT INTO sync_outbox VALUES('t','s','Counter','a',1);");assert(!read(db,"a")&&read(db,"b"));
    sql(db,"DELETE FROM sync_outbox; UPDATE sync_objects SET schema_fp='old' WHERE id='a';");assert(!read(db,"a"));
    sql(db,"UPDATE sync_objects SET schema_fp='',snapshot=X'00' WHERE id='a';");assert(!read(db,"a"));
    sql(db,"UPDATE sync_objects SET snapshot=X'',pruned_through=1 WHERE id='a';");assert(!read(db,"a"));
    sql(db,"UPDATE sync_objects SET revision=1,pruned_through=0 WHERE id='a';");assert(!read(db,"a"));
    sql(db,"UPDATE sync_objects SET schema_fp='v1',snapshot=X'0001' WHERE id='a';");assert(read(db,"a"));
    sql(db,"UPDATE sync_objects SET pruned_through=2 WHERE id='a';");assert(!read(db,"a"));
    sql(db,"UPDATE sync_objects SET pruned_through=1,epoch=0 WHERE id='a';");assert(!read(db,"a"));
    sql(db,"UPDATE sync_objects SET epoch=2 WHERE id='a';");assert(read(db,"a"));
    assert(sqlite3_close(db)==SQLITE_OK);std::cout<<"SNAPSHOT_ADMISSION_HISTORY_SQLITE_PASS\n";
}
