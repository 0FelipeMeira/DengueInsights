#
#   Run Project:
#   uvicorn main:app --host 0.0.0.0 --port 8000
#
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import os

app = FastAPI()
BASE_API_URL = "https://info.dengue.mat.br/api/alertcity"


@app.get("/dengue-dataset/", tags=["done"])
async def get_dengue_dataset():
    """
    Endpoint para ler e retornar todos os dados do arquivo dengue-dataset.csv.
    """
    file_path = os.path.join(os.getcwd(), "dengue-dataset.csv")

    # Verificar se o arquivo existe
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=404, detail="Arquivo 'dengue-dataset.csv' não encontrado"
        )

    try:
        # Ler o arquivo CSV usando Pandas
        df = pd.read_csv(file_path)

        df = df.fillna("N/A")
        df = df.replace([float("inf"), float("-inf")], "N/A")

        # Retornar os dados como JSON
        return JSONResponse(content=df.to_dict(orient="records"))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao ler o arquivo CSV: {str(e)}"
        )


@app.get("/dengue-alerts/", tags=["in dev"])
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
