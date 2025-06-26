# hybrid_multitenant.py

from fastapi import FastAPI, Request, Depends, HTTPException
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
import threading

# --- Central Metadata (Simulated for demo) ---
TENANT_META = {
    "alpha": {
        "db_url": "sqlite:///./alpha.db",
        "table_prefix": "alpha_"
    },
    "beta": {
        "db_url": "sqlite:///./beta.db",
        "table_prefix": "beta_"
    }
}

# --- Thread-safe tenant context ---
tenant_context = threading.local()

def get_tenant_meta(tenant_id: str):
    meta = TENANT_META.get(tenant_id)
    if not meta:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return meta

# --- Dynamic DB Session Factory ---
def get_db_session(tenant_id: str):
    meta = get_tenant_meta(tenant_id)
    engine = create_engine(meta["db_url"], echo=True, future=True)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)
    metadata = MetaData()
    tenant_context.prefix = meta["table_prefix"]
    return Session(), metadata, engine

# --- Table creation function ---
def ensure_tenant_table(engine, metadata, prefix):
    table_name = f"{prefix}users"
    users = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("email", String)
    )
    metadata.create_all(engine)
    return users

# --- FastAPI App Init ---
app = FastAPI()

class UserCreate(BaseModel):
    name: str
    email: str

# --- Dependency Injection: Get Session per request ---
def get_tenant_request(request: Request):
    tenant_id = request.headers.get("X-Tenant-ID")
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing X-Tenant-ID header")
    return tenant_id

# --- API Routes ---
@app.post("/users")
def create_user(user: UserCreate, tenant_id: str = Depends(get_tenant_request)):
    db, metadata, engine = get_db_session(tenant_id)
    prefix = tenant_context.prefix
    users_table = ensure_tenant_table(engine, metadata, prefix)

    with engine.connect() as conn:
        stmt = users_table.insert().values(name=user.name, email=user.email)
        conn.execute(stmt)
        conn.commit()
    return {"message": f"User created in tenant '{tenant_id}'"}

@app.get("/users")
def list_users(tenant_id: str = Depends(get_tenant_request)):
    db, metadata, engine = get_db_session(tenant_id)
    prefix = tenant_context.prefix
    users_table = ensure_tenant_table(engine, metadata, prefix)

    with engine.connect() as conn:
        stmt = users_table.select()
        result = conn.execute(stmt).fetchall()
        return [{"id": row.id, "name": row.name, "email": row.email} for row in result]
