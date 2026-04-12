from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery

app = FastAPI()

PROJECT_ID = "propertydb-492521"
DATASET = "Propertydb"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# CUSTOM ENDPOINTS (STATIC ROUTES MUST COME FIRST)
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# CUSTOM ENDPOINT: GET PROPERTIES BY CITY
# ---------------------------------------------------------------------------

@app.get("/properties/city/{city}")
def get_properties_by_city(city: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties located in the specified city.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(city) = LOWER(@city)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("city", "STRING", city)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No properties found in city '{city}'"
        )

    return [dict(row) for row in results]

# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a single property by ID.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    return dict(results[0])


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all income records for a property.
    """
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    income_records = [dict(row) for row in results]

    if not income_records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No income records found for property {property_id}"
        )

    return income_records


@app.post("/income/{property_id}")
def create_income(property_id: int, payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Creates a new income record for a property.
    """
    if "amount" not in payload or "date" not in payload:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request must include 'amount' and 'date'"
        )

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
        raise HTTPException(
            status_code=500,
            detail=f"Insert failed: {str(e)}"
        )

    return {"status": "success", "message": "Income record created"}


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    try:
        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            WHERE property_id = @property_id
            ORDER BY date DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )

        rows = bq.query(query, job_config=job_config).result()
        results = [dict(row) for row in rows]

        # Return empty list instead of 404 (frontend requirement)
        return results

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch expenses: {str(e)}"
        )

# ---------------------------------------------------------------------------
# PROPERTY FILTER ENDPOINTS (STATIC ROUTES — MUST COME BEFORE /properties/{property_id})
# ---------------------------------------------------------------------------

@app.get("/properties/city/{city}")
def get_properties_by_city(city: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties located in the specified city.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(city) = LOWER(@city)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("city", "STRING", city)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not results:
        raise HTTPException(404, f"No properties found in city '{city}'")

    return [dict(row) for row in results]


@app.get("/properties/state/{state}")
def get_properties_by_state(state: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties located in the specified state.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(state) = LOWER(@state)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("state", "STRING", state)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not results:
        raise HTTPException(404, f"No properties found in state '{state}'")

    return [dict(row) for row in results]


@app.get("/properties/postal/{postal_code}")
def get_properties_by_postal(postal_code: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties with the specified postal code.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE postal_code = @postal_code
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("postal_code", "STRING", postal_code)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not results:
        raise HTTPException(404, f"No properties found with postal code '{postal_code}'")

    return [dict(row) for row in results]


@app.get("/properties/tenant/{tenant_name}")
def get_properties_by_tenant(tenant_name: str, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties rented by the specified tenant.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            tenant_name
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE LOWER(tenant_name) = LOWER(@tenant_name)
        ORDER BY property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(500, f"Database query failed: {str(e)}")

    if not results:
        raise HTTPException(404, f"No properties found for tenant '{tenant_name}'")

    return [dict(row) for row in results]
