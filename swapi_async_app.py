import asyncio
from datetime import datetime
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy import Integer, String, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

import config

engine = create_async_engine(config.PG_DSN_ALC, echo=True)
Base = declarative_base(bind=engine)
DbSession = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

URL = 'https://swapi.dev/api/people/'

MAX = 100
PARTITION = 10

field_list = ["gender", "hair_color", "name", "skin_color", "birth_year", "eye_color", "height", "mass"]
field_list_url = ["species", "starships", "vehicles"]


class People(Base):
    __tablename__ = 'people'

    id = Column(Integer, primary_key=True)
    birth_year = Column(String(32))
    eye_color = Column(String(32))
    films = Column(String)
    gender = Column(String(32))
    hair_color = Column(String(32))
    height = Column(String(32))
    homeworld = Column(String(256))
    mass = Column(String(32))
    name = Column(String(64))
    skin_color = Column(String(64))
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)


def get_id(url: str):
    return int(url.split("/")[-2])


async def get_hero_name(session, home_url):
    async with session.get(home_url) as response:
        return await response.json()


async def get_names(session, urls):
    tasks = [asyncio.create_task(get_hero_name(session, url)) for url in urls]
    for task in tasks:
        name = await task
        yield name


async def urls_to_names(session, urls, obj_keyname="name"):
    result_list = []
    async for name in get_names(session, urls):
        result_list.append(name[obj_keyname])
    return ','.join(result_list)


async def specific_person(session, person):
    task = asyncio.create_task(get_hero_name(session, person["homeworld"]))
    name = await task
    res_person = {
        "id": get_id(person["url"]),
        "homeworld": name["name"]
    }
    for key in field_list:
        res_person[key] = person[key]
    for key in field_list_url:
        res_person[key] = await urls_to_names(session, person[key])
    res_person["films"] = await urls_to_names(session, person["films"], "title")
    return res_person


async def get_person(person_id, session):
    async with session.get(f'{URL}{person_id}') as response:
        return await response.json()


async def get_people(all_ids, partition, session):
    for chunk_ids in chunked(all_ids, partition):
        tasks = [asyncio.create_task(get_person(person_id, session)) for person_id in chunk_ids]
        for task in tasks:
            task_result = await task
            yield task_result


async def main():
    async with ClientSession() as http_session:
        async for people in get_people(range(1, MAX + 1), PARTITION, http_session):
            if "name" in people.keys():
                person = await specific_person(http_session, people)
                async with DbSession() as db_session:
                    db_session.add(People(**person))
                    await db_session.commit()


asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
start = datetime.now()
asyncio.run(main())
print(datetime.now() - start)
