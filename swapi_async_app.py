import asyncio
import asyncpg
from datetime import datetime
from aiohttp import ClientSession
from more_itertools import chunked

from sqlalchemy import Integer, String, Column, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import config

engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base(bind=engine)

URL = 'https://swapi.dev/api/people/'

MAX = 10
PARTITION = 2
SLEEP_TIME = 1


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(32))
    eye_color = Column(String(32))
    films = Column(ARRAY(String))
    gender = Column(String(32))
    hair_color = Column(String(32))
    height = Column(String(32))
    homeworld = Column(String(256))
    mass = Column(String(32))
    name = Column(String(64))
    skin_color = Column(String(64))
    species = Column(ARRAY(String))
    starships = Column(ARRAY(String))
    vehicles = Column(ARRAY(String))


async def get_async_session(
        drop: bool = False, create: bool = False
):
    async with engine.begin() as conn:
        if drop:
            await conn.run_sync(Base.metadata.drop_all)
        if create:
            print(1)
            await conn.run_sync(Base.metadata.create_all)
    async_session_maker = sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )

    return async_session_maker


async def get_person(person_id, session):
    async with session.get(f'{URL}{person_id}') as response:
        return await response.json()


async def get_people(all_ids, partition, session):
    for chunk_ids in chunked(all_ids, partition):
        tasks = [asyncio.create_task(get_person(person_id, session)) for person_id in chunk_ids]
        for task in tasks:
            task_result = await task
            yield task_result


async def insert_people(pool: asyncpg.Pool, people_list):
    # query = 'INSERT INTO people (birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles) ' \
    #         'VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)'
    # query = 'INSERT INTO people (birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles) ' \
    #         'VALUES ({people_list[0]}, {people_list[1]}, {people_list[2]}, {people_list[3]}, {people_list[4]}, {people_list[5]}, {people_list[6]}, {people_list[7]}, {people_list[8]}, {people_list[9]}, {people_list[10]}, {people_list[11]}, {people_list[12]})'
    # query = 'INSERT INTO people (birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles) VALUES (%(birth_year)s, %(eye_color)s, %(films)s, %(gender)s, %(hair_color)s, %(height)s, %(homeworld)s, %(mass)s, %(name)s, %(skin_color)s, %(species)s, %(starships)s, %(vehicles)s)'
    # query = """INSERT INTO people (birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    query = "INSERT INTO people (birth_year, eye_color, films, gender, hair_color, height, homeworld, mass, name, skin_color, species, starships, vehicles) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)"
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(query, people_list)


async def main():
    session = await get_async_session(False, True)
    db_tasks = []
    async with ClientSession() as http_session:
        pool = await asyncpg.create_pool(config.PG_DSN, min_size=2, max_size=2)
        async for people in get_people(range(1, MAX + 1), PARTITION, http_session):
            # print(people)
            # async with session() as db_session:
            #     async for result in people:
            #         db_session.add(People)
            #     await db_session.commit()
            # heroes_list = [people["birth_year"], people["eye_color"], people["films"], people["films"],
            #                people["hair_color"], people["height"], people["homeworld"], people["mass"],
            #                people["name"], people["skin_color"], people["species"], people["starships"],
            #                people["vehicles"]]
            # print(heroes_list)
            # await insert_people(pool, list(people))
            db_tasks.append(asyncio.create_task(insert_people(pool, list(people))))

            await asyncio.gather(*db_tasks)
            await pool.close()

            for task in db_tasks:
                await task

start = datetime.now()
asyncio.run(main())
print(datetime.now() - start)
