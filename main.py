# -*- coding: UTF-8 -*-
from fastapi import FastAPI, Form
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
from fastapi import FastAPI, Form, File, UploadFile

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount('/static', StaticFiles(directory='static'), name='static')


@app.post("/user/")
async def files(
                    request:           Request,
                    username: str    = Form(...),
                    password: str    = Form(...),
                ):
    print('username', username)
    print('password', password)
    if username == 'admin' and password == 'admin':

        return templates.TemplateResponse(
            'index.html',
            {
                'request':  request,
                'username': username,
            })
    else:
        return('Wrong pass')

@app.post("/file/")
async def upload_file(
                    request: Request, ## не понял зачем нужен
                    excel_file: UploadFile = File(...), ## наименование переменной должно быть одинаковым с наименованием  <input name="excel_file"
                ):
    return templates.TemplateResponse("accepted.html",
            {
                "request":      request,
              #  "file_sizes":   len(excel_file),
                "filenames":    excel_file.filename,
             })

@app.get("/")
async def main(request: Request):
    return templates.TemplateResponse('signin.html', {'request': request})



if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
