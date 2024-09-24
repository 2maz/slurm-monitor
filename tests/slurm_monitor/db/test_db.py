from slurm_monitor.db.v1.db_tables import GPUStatus

def test_gpu_infos(test_db, number_of_nodes, number_of_gpus):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"
        gpu_infos = test_db.get_gpu_infos(node=nodename)

        assert "gpus" in gpu_infos
        assert len(gpu_infos["gpus"]) == number_of_gpus

def test_gpu_status(test_db, number_of_nodes, number_of_gpus, number_of_samples):
    for i in range(0, number_of_nodes):
        nodename = f"node-{i}"

        resolution_in_s = 10
        gpu_status = test_db.get_gpu_status(node=nodename, resolution_in_s=resolution_in_s)
        assert len(gpu_status) >= (number_of_samples / resolution_in_s)

def test_dataframe(test_db, number_of_gpus, number_of_samples):
    uuids = test_db.get_gpu_uuids(node="node-1")
    df = test_db._fetch_dataframe(GPUStatus, GPUStatus.uuid.in_(uuids))
    assert len(df) == number_of_gpus*number_of_samples
