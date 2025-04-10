import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from slurm_monitor.db.v1.db_tables import GPUStatus, ProcessStatus, TableBase

#@pytest.mark.asyncio(loop_scope="module")
#async def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
#
#    async with AsyncSession(test_db.async_engine) as session:
#        async with session.begin():
#            stmt = select(GPUStatus)
#            results = await session.execute(stmt)
#            print(f"Results: {results.all()}")

#@pytest.mark.asyncio(loop_scope="module")
#async def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
#    #session = test_db.async_session_factory()
#    #async with session: #test_db.make_async_session() as session:
#    async with test_db.make_async_session() as session:
#        stmt = select(GPUStatus)
#        results = await session.execute(stmt)
#        print(f"Results: {results.all()}")

@pytest.mark.asyncio(loop_scope="module")
async def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
    results = await test_db.fetch_all_async(GPUStatus)
    print(f"Results: {results}")

@pytest.mark.asyncio(loop_scope="module")
async def test_fetch_first(test_db, number_of_nodes, number_of_gpus):
    results = await test_db.fetch_first_async(GPUStatus, order_by=GPUStatus.timestamp.desc())
    assert results.timestamp == test_db.fetch_first(GPUStatus, order_by=GPUStatus.timestamp.desc()).timestamp

@pytest.mark.asyncio(loop_scope="module")
async def test_fetch_latest(test_db, number_of_nodes, number_of_gpus):
    results = await test_db.fetch_latest_async(GPUStatus)
    assert results == test_db.fetch_latest(GPUStatus)

@pytest.mark.asyncio(loop_scope="module")
async def test_node_last_probe_timestamp(test_db, number_of_nodes, number_of_gpus):
    results = await test_db.get_last_probe_timestamp()
    assert len(results) > 0

@pytest.mark.asyncio(loop_scope="module")
async def test_get_gpu_status_timeseries_list(test_db, number_of_nodes, number_of_gpus):
    results = await test_db.get_gpu_status_timeseries_list()
    assert len(results) > 0
    print(results)

@pytest.mark.asyncio(loop_scope="module")
async def test_get_cpu_status_timeseries_list(test_db, number_of_nodes, number_of_gpus):
    results = await test_db.get_cpu_status_timeseries_list()
    assert len(results) > 0
    print(results)


    #results = await test_db.fetch_all_async(GPUStatus)
    #print(results)
    #assert results is not None

    #for i in range(0, number_of_nodes):
    #    nodename = f"node-{i}"
    #    #gpu_infos = test_db.get_gpu_infos(node=nodename)

    #    #assert "gpus" in gpu_infos
    #    #assert len(gpu_infos["gpus"]) == number_of_gpus

#def test_gpu_status(test_db, number_of_nodes, number_of_gpus, number_of_samples):
#    for i in range(0, number_of_nodes):
#        nodename = f"node-{i}"
#
#        resolution_in_s = 10
#        gpu_status = test_db.get_gpu_status(node=nodename, resolution_in_s=resolution_in_s)
#        assert len(gpu_status) >= (number_of_samples / resolution_in_s)
#
#def test_dataframe(test_db, number_of_gpus, number_of_samples):
#    uuids = test_db.get_gpu_uuids(node="node-1")
#    assert len(uuids) == number_of_gpus
#
#    df = test_db._fetch_dataframe(GPUStatus, GPUStatus.uuid.in_(uuids))
#    assert len(df) == number_of_gpus*number_of_samples
#
#
#def test_apply_resolution_GPUStatus(test_db):
#    data = test_db.fetch_all(GPUStatus)
#    TableBase.apply_resolution(data, resolution_in_s=100)
#
#def test_apply_resolution_ProcessStatus(test_db):
#    data = test_db.fetch_all(ProcessStatus)
#    TableBase.apply_resolution(data, resolution_in_s=100)
