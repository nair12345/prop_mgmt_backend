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

    properties = [dict(row) for row in results]
    return properties
    
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
def create_income(
    property_id: int,
    payload: dict,
    bq: bigquery.Client = Depends(get_bq_client)
):
    """
    Creates a new income record for a property.
    Expected JSON body:
    {
        "amount": 1200.00,
        "date": "2024-01-15",
        "description": "January Rent"
    }
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
            bigquery.ScalarQueryParameter(
                "description",
                "STRING",
                payload.get("description", None)
            ),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Insert failed: {str(e)}"
        )

    return {"status": "success", "message": "Income record created"}

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int):
    try:
        query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            WHERE property_id = {property_id}
            ORDER BY date DESC
        """
        rows = client.query(query).result()
        results = [dict(row) for row in rows]

        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No expenses found for property {property_id}"
            )

        return results

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch expenses: {str(e)}"
        )
        
@app.post("/expenses/{property_id}")
def create_expense(property_id: int, expense: Expense):
    try:
        query = f"""
            INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
            (property_id, amount, date, category, vendor, description)
            VALUES (
                {property_id},
                {expense.amount},
                '{expense.date}',
                '{expense.category}',
                {f"'{expense.vendor}'" if expense.vendor else "NULL"},
                {f"'{expense.description}'" if expense.description else "NULL"}
            )
        """

        client.query(query).result()

        return {"status": "success", "message": "Expense record created"}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create expense: {str(e)}"
        )
# -----------------------------
# GET 1: /properties/addresses
# Returns all property IDs + addresses
# -----------------------------
@app.get("/properties/addresses")
def get_property_addresses():
    try:
        query = f"""
            SELECT property_id, address
            FROM `{PROJECT_ID}.{DATASET}.properties`
            ORDER BY property_id
        """
        rows = client.query(query).result()
        return [dict(row) for row in rows]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch property addresses: {str(e)}"
        )
