import asyncio
from datasette.app import Datasette
import pytest
import pytest_asyncio


@pytest_asyncio.fixture()
async def datasette():
    datasette = Datasette()
    db = datasette.add_memory_database("demo")
    # Drop all tables
    for table in await db.table_names():
        await db.execute_write("drop table {}".format(table))
    await db.execute_write("create table news (body text)")
    for text in ("example a", "example b", "example c"):
        await db.execute_write("insert into news (body) values (?)", [text])
    return datasette


async def _cookies(datasette):
    cookies = {"ds_actor": datasette.client.actor_cookie({"id": "root"})}
    csrftoken = (
        await datasette.client.get("/-/enrich/demo/news/re2", cookies=cookies)
    ).cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    return cookies


@pytest.mark.asyncio
async def test_single(datasette: Datasette):
    cookies = await _cookies(datasette)
    response = await datasette.client.post(
        "/-/enrich/demo/news/re2",
        data={
            "source_column": "body",
            "regex": r"example (?P<letter>[a-z])",
            "single_column": "letter",
            "mode": "single",
            "csrftoken": cookies["ds_csrftoken"],
        },
        cookies=cookies,
    )
    assert response.status_code == 302
    # Wait 0.5s and the enrichment should have run
    await asyncio.sleep(0.5)
    db = datasette.get_database("demo")
    jobs = await db.execute("select * from _enrichment_jobs")
    job = dict(jobs.first())
    assert job["status"] == "finished"
    assert job["enrichment"] == "re2"
    assert job["done_count"] == 3
    results = await db.execute("select body, letter from news order by body")
    rows = [dict(r) for r in results.rows]
    assert rows == [
        {"body": "example a", "letter": "a"},
        {"body": "example b", "letter": "b"},
        {"body": "example c", "letter": "c"},
    ]


@pytest.mark.asyncio
async def test_replCE(datasette: Datasette):
    cookies = await _cookies(datasette)
    response = await datasette.client.post(
        "/-/enrich/demo/news/re2",
        data={
            "source_column": "body",
            "regex": r"example (?P<letter>[a-z])",
            "single_column": "",  # Blank means same column
            "mode": "replace",
            "replacement": r"replaced \1",
            "csrftoken": cookies["ds_csrftoken"],
        },
        cookies=cookies,
    )
    assert response.status_code == 302
    # Wait 0.5s and the enrichment should have run
    await asyncio.sleep(0.5)
    db = datasette.get_database("demo")
    jobs = await db.execute("select * from _enrichment_jobs")
    job = dict(jobs.first())
    assert job["status"] == "finished"
    assert job["enrichment"] == "re2"
    assert job["done_count"] == 3
    results = await db.execute("select body from news order by body")
    rows = [dict(r) for r in results.rows]
    assert rows == [
        {"body": "replaced a"},
        {"body": "replaced b"},
        {"body": "replaced c"},
    ]
