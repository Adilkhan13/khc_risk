# -*- coding: UTF-8 -*-
from datetime import date
import functions
from fastapi import FastAPI, Form
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from fastapi import FastAPI, Form, File, UploadFile
from fastapi.responses import FileResponse
import pandas as pd
from pathlib import Path
import logging


logging.basicConfig(
    filename="app.log",
    filemode="w",
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


# mypackage
app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.post("/user/")
async def files(
    request: Request, username: str = Form(...), password: str = Form(...),
):
    logging.info(f"username {str(username)}, pass {str(password)}")

    # pass
    if username == "admin" and password == "admin":
        df = pd.read_csv("./datasets/loggs_k.csv")
        df["отчетная дата"] = pd.to_datetime(df["отчетная дата"], format="%d.%m.%y")
        last_report_date = df["отчетная дата"].max()
        logging.info(f"last_report_date {str(last_report_date)[:10]}")
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "last_report_date": str(last_report_date)[:10],},
        )
    else:
        logging.info(f"Wrong pass username {username}, pass {password}")
        return "Wrong pass"


@app.post("/file/")
async def upload_file(
    request: Request,  # не понял зачем нужен
    # наименование переменной должно быть одинаковым с наименованием  <input name="excel_file"
    excel_file: UploadFile = File(...),
):
    logging.info(f"uploading file {excel_file.filename}")
    suffix = Path(excel_file.filename).suffix
    if suffix != ".xlsx" and suffix != ".xls":
        return {"info": f"file have not supported format = {suffix}"}
    else:
        file_location = f"./uploads/{excel_file.filename}"
        with open(file_location, "wb+") as file_object:
            file_object.write(excel_file.file.read())
        try:
            functions.add_merge(file_location)
        except Exception as e:
            logging.exception("Exception occurred")
            logging.error(f"file '{excel_file.filename}' not supported")
            return {"info": f"file '{excel_file.filename}' not supported"}
        df = functions.load_data()
        ids = functions.ids(df)
        functions.vintage(df, ids).to_excel(
            "./datasets/vintage/vintage_кик_" + str(date.today()) + ".xlsx"
        )
        logging.info(f"Обновление прошло успешно")
        return {"info": f"Обновление прошло успешно"}


@app.post("/download/")
async def download_file(request: Request,):  # не понял зачем нужен
    # временно статичным
    logging.info("Скачивание винтажа")
    file_location = functions.newest(path="./datasets/vintage")

    return FileResponse(file_location)


@app.get("/")
async def main(request: Request):
    return templates.TemplateResponse("signin.html", {"request": request})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="192.168.5.68", port=8000, reload=True)
