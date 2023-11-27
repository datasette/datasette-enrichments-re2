import asyncio
from datasette.app import Datasette
import pytest
import pytest_asyncio


@pytest_asyncio.fixture()
async def datasette():
    datasette = Datasette()
    db = datasette.add_memory_database("demo")
    await db.execute_write("create table news (body text)")
    for text in ("example a", "example b", "example c"):
        await db.execute_write("insert into news (body) values (?)", [text])
    return datasette


@pytest.mark.asyncio
async def test_enrich_re2(datasette: Datasette):
    cookies = {"ds_actor": datasette.client.actor_cookie({"id": "root"})}
    csrftoken = (
        await datasette.client.get("/-/enrich/demo/news/re2", cookies=cookies)
    ).cookies["ds_csrftoken"]
    cookies["ds_csrftoken"] = csrftoken
    response = await datasette.client.post(
        "/-/enrich/demo/news/re2",
        data={
            "source_column": "body",
            "regex": r"example (?P<letter>[a-z])",
            "single_column": "letter",
            "mode": "single",
            "csrftoken": csrftoken,
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
