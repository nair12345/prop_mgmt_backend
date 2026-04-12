from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware # 1. Import this
from google.cloud import bigquery

app = FastAPI()

# 2. Add this block right after app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins (good for development/testing)
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"], # Allows all headers
)

PROJECT_ID = "propertydb-492521"
DATASET = "Propertydb"

# ---------------------------------------------------------------------------
# BigQuery Client Dependency
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# STATIC ROUTES (MUST COME BEFORE /properties/{property_id})
# ---------------------------------------------------------------------------

@app.get("/properties/city/{city}")
def get_properties_by_city(city: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(city) = LOWER(@city)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("city", "STRING", city)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not rows:
        raise HTTPException(404, f"No properties found in city '{city}'")

    return [dict(r) for r in rows]


@app.get("/properties/state/{state}")
def get_properties_by_state(state: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(state) = LOWER(@state)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("state", "STRING", state)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not rows:
        raise HTTPException(404, f"No properties found in state '{state}'")

    return [dict(r) for r in rows]


@app.get("/properties/postal/{postal_code}")
def get_properties_by_postal(postal_code: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state, postal_code
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE postal_code = @postal_code
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("postal_code", "STRING", postal_code)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not rows:
        raise HTTPException(404, f"No properties found with postal code '{postal_code}'")

    return [dict(r) for r in rows]


@app.get("/properties/tenant/{tenant_name}")
def get_properties_by_tenant(tenant_name: str, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state, tenant_name
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(tenant_name) = LOWER(@tenant_name)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not rows:
        raise HTTPException(404, f"No properties found for tenant '{tenant_name}'")

    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# PROPERTIES
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state, postal_code,
               property_type, tenant_name, monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        rows = bq.query(query).result()
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    return [dict(r) for r in rows]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT property_id, name, address, city, state, postal_code,
               property_type, tenant_name, monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not rows:
        raise HTTPException(404, f"Property with ID {property_id} not found")

    return dict(rows[0])


# ---------------------------------------------------------------------------
# INCOME
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT income_id, property_id, amount, date, description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    return rows  # return empty list if none


@app.post("/income/{property_id}")
def create_income(property_id: int, payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    if "amount" not in payload or "date" not in payload:
        raise HTTPException(400, "Request must include 'amount' and 'date'")

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income`
        (property_id, amount, date, description)
        VALUES (@property_id, @amount, @date, @description)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", payload["amount"]),
            bigquery.ScalarQueryParameter("date", "DATE", payload["date"]),
            bigquery.ScalarQueryParameter("description", "STRING", payload.get("description")),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(500, f"Insert failed: {str(e)}")

    return {"status": "success", "message": "Income record created"}


# ---------------------------------------------------------------------------
# EXPENSES (FIXED)
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT expense_id, property_id, amount, date, category, vendor, description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("property_id", "INT64", property_id)]
    )

    try:
        rows = list(bq.query(query, job_config=job_config).result())
        return rows  # empty list allowed
    except Exception as e:
        raise HTTPException(500, f"Failed to fetch expenses: {str(e)}")

@app.post("/expenses/{property_id}")
def create_expense(property_id: int, payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new expense record for a property.
    """
    # Required fields
    if "amount" not in payload or "date" not in payload or "category" not in payload:
        raise HTTPException(
            status_code=400,
            detail="Request must include 'amount', 'date', and 'category'"
        )

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
        (property_id, amount, date, category, vendor, description)
        VALUES (@property_id, @amount, @date, @category, @vendor, @description)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("amount", "FLOAT64", payload["amount"]),
            bigquery.ScalarQueryParameter("date", "DATE", payload["date"]),
            bigquery.ScalarQueryParameter("category", "STRING", payload["category"]),
            bigquery.ScalarQueryParameter("vendor", "STRING", payload.get("vendor")),
            bigquery.ScalarQueryParameter("description", "STRING", payload.get("description")),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Insert failed: {str(e)}"
        )

    return {"status": "success", "message": "Expense record created"}


