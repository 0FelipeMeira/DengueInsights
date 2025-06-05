#
#   Run Project:
#   pip install -r requirements.txt
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import sqlite3
import httpx
import pandas as pd
import os

app = FastAPI()
BASE_API_URL = "https://info.dengue.mat.br/api/alertcity"


@app.get("/dengue-dataset/", tags=["done"])
async def get_dengue_dataset():
    file_path = os.path.join(os.getcwd(), "dengue-dataset.csv")

    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404, detail="Arquivo 'dengue-dataset.csv' não encontrado"
        )

    try:
        df = pd.read_csv(file_path)

        df = df.fillna("N/A")
        df = df.replace([float("inf"), float("-inf")], "N/A")

        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao ler o arquivo CSV: {str(e)}"
        )


@app.get("/dengue-alerts/", tags=["done"])
async def get_dengue_alerts():
    doenca = "dengue"
    geocode = "3509502"
    data_format = "json"
    api_url = f"{BASE_API_URL}?disease={doenca}&geocode={geocode}&format={data_format}"

    async with httpx.AsyncClient() as client:
        response = await client.get(api_url)

    if response.status_code != 200:
        raise HTTPException(
            status_code=500, detail="Erro ao obter dados da API externa"
        )

    data = response.json()

    df = pd.DataFrame(data)

    if df.empty:
        raise HTTPException(
            status_code=404, detail="Nenhum alerta encontrado com esses critérios"
        )

    return df.to_json(orient="records")

@app.post("/dengue-dataset/save/", tags=["done"])
async def save_dengue_dataset_to_db():
    csv_path = os.path.join(os.getcwd(), "dengue-dataset.csv")
    db_path = os.path.join(os.getcwd(), "dengue_data.db")

    if not os.path.exists(csv_path):
        raise HTTPException(
            status_code=404, detail="Arquivo 'dengue-dataset.csv' não encontrado"
        )

    try:
        df = pd.read_csv(csv_path)
        df = df.fillna("N/A")
        df = df.replace([float("inf"), float("-inf")], "N/A")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        table_name = "dengue_data"

        df.to_sql(table_name, conn, if_exists="replace", index=False)

        conn.commit()
        conn.close()

        return {"message": "Dados salvos no banco de dados com sucesso."}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao salvar os dados no banco de dados: {str(e)}",
        )