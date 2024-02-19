from sqlalchemy import Column, Integer, String, func, ForeignKey, DATETIME, create_engine
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
import os
import catboost
from fastapi import FastAPI, Depends
from typing import List
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session


engine = create_engine(    )         #Здесь были прописанн путь для подключения к серверу на котором хранились данные о пользователях

conn = engine.connect().execution_options(stream_results=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class Post(Base):
    ''' Класс таблицы постов'''
    
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    text = Column(String)
    topic = Column(String)



class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True



def get_model_path(path: str) -> str:
    ''' Функция которая возвращает путь по которому получаем модель для предсказания топ постов
        Если на сторонем сервере, то один путь, если локально, то другой'''
    
    if os.environ.get("IS_LMS") == "1":
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH


def load_models():
    ''' Функция загрузки модели, локально она хранилась как указанно ниже'''
    
    model_path = get_model_path("D:\Download\catboost_model")
    from_file = catboost.CatBoostClassifier()
    model = from_file.load_model(model_path)
    return model


def batch_load_sql(query: str) -> pd.DataFrame:
    '''Функция для загрузки предобработанных таблиц, с которыми в дальнейшем взаимодействует модель. 
        Отдельная функция сделана, чтобы не тратить лишнюю память при загрузки слишком больших таблиц'''
    
    CHUNKSIZE = 200000
    engine = create_engine()        #Здесь были прописанн путь для подключения к серверу на котором хранились данные о пользователях
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)


def load_features() -> pd.DataFrame:
    ''' Функция для загрузки моих личных предобработанных таблиц, с которыми в дальнейшем взаимодействует модель'''
    
    my_query = 'SELECT * FROM a_klabukov_lesson_22_user'
    return batch_load_sql(my_query)


post = batch_load_sql('SELECT * FROM a_klabukov_lesson_22_copy_1') #Загрузка предобработанной таблицы постов
clf = load_models()   #Загрузка модели
us_tab_2 = pd.read_sql('SELECT * FROM a_klabukov_user_3', con=conn) #Загрузка предобработанной таблицы пользователей



def predict_proba_id(id: int, time: datetime):
    ''' Функция для возвращения предсказания модели. Для работы модели на вход подавался id пользователя и время.
        Сначала в датафрейме постов добавляем колонку id. Далее мерджим таблицу с данными о самом пользователе.
        Добавляем колонку дня недели, и часа. Меняем индексы на id постов, потому что они в дальейшем понадобятся.
        Дропаем лишние колонки. Делаем предсказание моделиб сортируем и возвраем кортеж топ5 постов, которые лайкнет 
        пользователь, по предсказанию модели'''
    
    df_post_like1000 = post.copy()
    df_post_like1000['user_id'] = id
    df_post_like1000 = pd.merge(df_post_like1000, us_tab_2, on='user_id')
    df_post_like1000['day_of_week'] = time.weekday()
    df_post_like1000['hour'] = time.timetuple()[3]
    df_post_like1000 = df_post_like1000.set_index(df_post_like1000['post_id'])
    df_post_like1000 = df_post_like1000.drop(['user_id', 'post_id', 'text', 'age'], axis=1)
    df_post_like1000['proba'] = [i[1] for i in clf.predict_proba(df_post_like1000)]
    ls = (df_post_like1000.sort_values(['proba'])['proba'].index[-1:-6:-1])
    return tuple(ls)



app = FastAPI()

def get_db():
    with SessionLocal() as db:
        return db


@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int,
		time: datetime,
        db: Session = Depends(get_db)) -> List[PostGet]:
    '''Функция для возвращения топ 5 постов в виде списка объектов PostGet'''

    ls = predict_proba_id(id, time)
    return  db.query(Post).filter(Post.id.in_((ls))).all()