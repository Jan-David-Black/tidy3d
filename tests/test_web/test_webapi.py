# Tests webapi and things that depend on it
import tempfile
import pytest
import responses
import tidy3d as td
import tidy3d.web as web

from responses import matchers

from tidy3d import Simulation
from tidy3d.web.environment import Env
from tidy3d.web.webapi import delete, delete_old, download, download_json, run
from tidy3d.web.webapi import download_log, estimate_cost, get_info, get_run_info, get_tasks
from tidy3d.web.webapi import load, load_simulation, start, upload, monitor, real_cost
from tidy3d.web.container import Job, Batch
from tidy3d.web.task import TaskInfo
from tidy3d.web.asynchronous import run_async

from tidy3d.__main__ import main

# variables used below
FNAME_TMP = "tests/tmp/web_test_tmp.json"
TASK_NAME = "task_name_test"
TASK_ID = "1234"
CREATED_AT = "2022-01-01T00:00:00.000Z"
PROJECT_NAME = "default"
FLEX_UNIT = 1.0


def make_sim():
    """Makes a simulation."""
    return td.Simulation(size=(1, 1, 1), grid_spec=td.GridSpec.auto(wavelength=1.0), run_time=1e-12)


@pytest.fixture
def set_api_key(monkeypatch):
    """Set the api key."""
    import tidy3d.web.http_management as http_module

    monkeypatch.setattr(http_module, "api_key", lambda: "apikey")


@pytest.fixture
def mock_upload(monkeypatch, set_api_key):
    """Mocks webapi.uupload."""

    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/project",
        match=[matchers.query_param_matcher({"projectName": PROJECT_NAME})],
        json={"data": {"projectId": TASK_ID, "projectName": PROJECT_NAME}},
        status=200,
    )

    responses.add(
        responses.POST,
        f"{Env.current.web_api_endpoint}/tidy3d/projects/{TASK_ID}/tasks",
        match=[
            matchers.json_params_matcher(
                {
                    "taskName": TASK_NAME,
                    "call_back_url": None,
                    "simulationType": "tidy3d",
                    "parentTasks": None,
                }
            )
        ],
        json={
            "data": {
                "taskId": TASK_ID,
                "taskName": TASK_NAME,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def mock_download(*args, **kwargs):
        pass

    monkeypatch.setattr("tidy3d.web.simulation_task.upload_string", mock_download)


@pytest.fixture
def mock_get_info(monkeypatch, set_api_key):
    """Mocks webapi.get_info."""

    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
                "realFlexUnit": FLEX_UNIT,
            }
        },
        status=200,
    )


@pytest.fixture
def mock_start(monkeypatch, set_api_key):
    """Mocks webapi.start."""

    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/submit",
        match=[
            matchers.json_params_matcher(
                {
                    "solverVersion": None,
                    "workerGroup": None,
                    "protocolVersion": td.version.__version__,
                }
            )
        ],
        json={
            "data": {
                "taskId": TASK_ID,
                "taskName": TASK_NAME,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )


@pytest.fixture
def mock_monitor(monkeypatch):

    status_count = [0]
    statuses = ("upload", "running", "running", "running", "running", "running", "success")

    def mock_get_info(task_id):

        current_count = min(status_count[0], len(statuses) - 1)
        current_status = statuses[current_count]
        status_count[0] += 1
        return TaskInfo(status=current_status, taskName=TASK_NAME, taskId=task_id, realFlexUnit=1.0)

    run_count = [0]
    perc_dones = (1, 10, 20, 30, 100)

    def mock_get_run_info(task_id):
        current_count = min(run_count[0], len(perc_dones) - 1)
        perc_done = perc_dones[current_count]
        run_count[0] += 1
        return perc_done, 1

    monkeypatch.setattr("tidy3d.web.webapi.REFRESH_TIME", 0.00001)
    monkeypatch.setattr("tidy3d.web.webapi.RUN_REFRESH_TIME", 0.00001)
    monkeypatch.setattr("tidy3d.web.webapi.get_info", mock_get_info)
    monkeypatch.setattr("tidy3d.web.webapi.get_run_info", mock_get_run_info)


@pytest.fixture
def mock_download(monkeypatch, set_api_key):
    """Mocks webapi.download."""
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def _mock_download(*args, **kwargs):
        file_path = kwargs["to_file"]
        with open(file_path, "w") as f:
            f.write("0.3,5.7")

    monkeypatch.setattr("tidy3d.web.simulation_task.download_file", _mock_download)
    download(TASK_ID, FNAME_TMP)
    with open(FNAME_TMP, "r") as f:
        assert f.read() == "0.3,5.7"


@pytest.fixture
def mock_load(monkeypatch, set_api_key):
    """Mocks webapi.load"""

    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def _mock_download(*args, **kwargs):
        pass

    monkeypatch.setattr("tidy3d.web.simulation_task.download_file", _mock_download)

    # estimate cost
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )


responses.add(
    responses.POST,
    f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/metadata",
    json={
        "data": {
            "flex_unit": 11.11,
            "createdAt": CREATED_AT,
        }
    },
    status=200,
)


@pytest.fixture
def mock_get_run_info(monkeypatch, set_api_key):
    """Mocks webapi.get_run_info"""
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/progress",
        json={
            "data": {
                "perc_done": 100,
                "field_decay": 0,
            }
        },
        status=200,
    )


@pytest.fixture
def mock_webapi(mock_upload, mock_get_info, mock_start, mock_monitor, mock_download, mock_load):
    """Mocks all webapi operation."""


@responses.activate
def test_upload(mock_upload):
    sim = make_sim()
    assert upload(sim, TASK_NAME, PROJECT_NAME)


@responses.activate
def test_get_info(mock_get_info):
    assert get_info(TASK_ID).taskId == TASK_ID


@responses.activate
def test_start(mock_start):
    start(TASK_ID)


@responses.activate
def test_get_run_info(mock_get_run_info):
    assert get_run_info(TASK_ID) == (100, 0)


@responses.activate
def test_download(mock_download):
    download(TASK_ID, FNAME_TMP)
    with open(FNAME_TMP, "r") as f:
        assert f.read() == "0.3,5.7"


@responses.activate
def _test_load(mock_load):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def mock_download(*args, **kwargs):
        pass

    monkeypatch.setattr("tidy3d.web.simulation_task.download_file", mock_download)
    load(TASK_ID, "tmp/monitor_data.hdf5")


@responses.activate
def test_delete(set_api_key):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    responses.add(
        responses.DELETE,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    assert delete(TASK_ID).taskId == TASK_ID


@responses.activate
def test_estimate_cost(set_api_key):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )
    responses.add(
        responses.POST,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/metadata",
        json={
            "data": {
                "flex_unit": 11.11,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )
    assert estimate_cost(TASK_ID) == 11.11


@responses.activate
def test_download_json(monkeypatch):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def mock_download(*args, **kwargs):
        file_path = kwargs["to_file"]
        with open(file_path, "w") as f:
            f.write("0.3,5.7")

    monkeypatch.setattr("tidy3d.web.simulation_task.download_file", mock_download)

    download_json(TASK_ID, FNAME_TMP)
    with open(FNAME_TMP, "r") as f:
        assert f.read() == "0.3,5.7"


@responses.activate
def test_load_simulation(monkeypatch):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def mock_download(*args, **kwargs):
        make_sim().to_file(args[1])

    monkeypatch.setattr(
        "tidy3d.web.simulation_task.SimulationTask.get_simulation_json", mock_download
    )

    assert load_simulation(TASK_ID, FNAME_TMP + ".json")


@responses.activate
def test_download_log(monkeypatch):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}/detail",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    def mock(*args, **kwargs):
        file_path = kwargs["to_file"]
        with open(file_path, "w") as f:
            f.write("0.3,5.7")

    monkeypatch.setattr("tidy3d.web.simulation_task.download_file", mock)

    download_log(TASK_ID, FNAME_TMP)
    with open(FNAME_TMP, "r") as f:
        assert f.read() == "0.3,5.7"


@responses.activate
def test_delete_old(set_api_key):
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/project",
        match=[matchers.query_param_matcher({"projectName": PROJECT_NAME})],
        json={"data": {"projectId": TASK_ID, "projectName": PROJECT_NAME}},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/projects/{TASK_ID}/tasks",
        json={"data": [{"taskId": TASK_ID, "createdAt": CREATED_AT}]},
        status=200,
    )

    responses.add(
        responses.DELETE,
        f"{Env.current.web_api_endpoint}/tidy3d/tasks/{TASK_ID}",
        json={
            "data": {
                "taskId": TASK_ID,
                "createdAt": CREATED_AT,
            }
        },
        status=200,
    )

    delete_old(100)


@responses.activate
def test_get_tasks(set_api_key):

    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/project",
        match=[matchers.query_param_matcher({"projectName": PROJECT_NAME})],
        json={"data": {"projectId": TASK_ID, "projectName": PROJECT_NAME}},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{Env.current.web_api_endpoint}/tidy3d/projects/{TASK_ID}/tasks",
        json={"data": [{"taskId": TASK_ID, "createdAt": CREATED_AT}]},
        status=200,
    )

    assert get_tasks(1)[0]["task_id"] == TASK_ID


@responses.activate
def test_run(mock_webapi, monkeypatch):
    sim = make_sim()
    monkeypatch.setattr("tidy3d.web.webapi.load", lambda *args, **kwargs: True)
    assert run(sim, task_name=TASK_NAME, folder_name=PROJECT_NAME, path=FNAME_TMP)


@responses.activate
def test_monitor(mock_monitor):
    monitor(TASK_ID, verbose=True)
    monitor(TASK_ID, verbose=False)


@responses.activate
def test_real_cost(mock_get_info):
    assert real_cost(TASK_ID) == FLEX_UNIT


""" Containers """


@responses.activate
def test_job(mock_webapi, monkeypatch):

    monkeypatch.setattr("tidy3d.web.container.Job.load", lambda *args, **kwargs: True)
    sim = make_sim()
    j = Job(simulation=sim, task_name=TASK_NAME, folder_name=PROJECT_NAME)

    sim_data = j.run(path=FNAME_TMP)
    j.status
    j.get_info()
    # j.download
    j.delete
    assert j.real_cost() == FLEX_UNIT


@pytest.fixture
def mock_job_status(monkeypatch):
    monkeypatch.setattr("tidy3d.web.container.Job.status", property(lambda self: "success"))
    monkeypatch.setattr("tidy3d.web.container.Job.load", lambda *args, **kwargs: True)


@responses.activate
def test_batch(mock_webapi, mock_job_status):

    # monkeypatch.setattr("tidy3d.web.container.Batch.monitor", lambda self: time.sleep(0.1))
    # monkeypatch.setattr("tidy3d.web.container.Job.status", property(lambda self: "success"))

    sims = {TASK_NAME: make_sim()}
    b = Batch(simulations=sims, folder_name=PROJECT_NAME)
    batch_data = b.run(path_dir="tests/tmp/")
    assert b.real_cost() == FLEX_UNIT * len(sims)


""" Async """


@responses.activate
def test_async(mock_webapi, mock_job_status):

    # monkeypatch.setattr("tidy3d.web.container.Job.status", property(lambda self: "success"))
    sims = {TASK_NAME: make_sim()}
    batch_data = run_async(sims, folder_name=PROJECT_NAME)


""" Main """


@responses.activate
def test_main(mock_webapi, monkeypatch, mock_job_status):

    # sims = {TASK_NAME: make_sim()}
    # batch_data = run_async(sims, folder_name=PROJECT_NAME)

    def save_sim_to_path(path: str) -> None:
        sim = Simulation(size=(1, 1, 1), grid_spec=td.GridSpec.auto(wavelength=1.0), run_time=1e-12)
        sim.to_file(path)

    def mock_get_info(task_id):
        return TaskInfo(
            status="success",
            taskName=TASK_NAME,
            taskId=TASK_ID,
            realFlexUnit=1.0,
            s3Storage=1.0,
            estFlexUnit=1.0,
        )

    monkeypatch.setattr("tidy3d.web.webapi.get_info", mock_get_info)
    monkeypatch.setattr("builtins.input", lambda _: "Y")

    path = f"tests/tmp/sim.json"
    save_sim_to_path(path)
    main(
        [
            path,
            "--task_name",
            TASK_NAME,
            "--folder_name",
            PROJECT_NAME,
            "--inspect_credits",
            "--inspect_sim",
        ]
    )

    monkeypatch.setattr("builtins.input", lambda _: "N")
    with pytest.raises(SystemExit):
        main(
            [
                path,
                "--task_name",
                TASK_NAME,
                "--folder_name",
                PROJECT_NAME,
                "--inspect_credits",
            ]
        )

    with pytest.raises(SystemExit):
        main(
            [
                path,
                "--task_name",
                TASK_NAME,
                "--folder_name",
                PROJECT_NAME,
                "--inspect_sim",
            ]
        )
