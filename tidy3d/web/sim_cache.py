from tidy3d.web.simulation_task import Folder, SimulationTask
import tidy3d.web.s3utils as s3
import json, tempfile, os
from functools import cache
from botocore.exceptions import ClientError
from tqdm.notebook import tqdm
SIM_CACHE='sim_cache'

@cache
def cache_task_id():
  cache_folder = Folder.get(SIM_CACHE)
  if cache_folder and cache_folder.list_tasks():
    print("found existing")
    task = cache_folder.list_tasks()[0]
  else:
    print("creating new task")
    task = SimulationTask.create(None,SIM_CACHE,SIM_CACHE)
  return task.task_id

def download_sim_cache():
  with tempfile.NamedTemporaryFile(suffix=".json") as temp:
    try:
      s3.download_file(
        cache_task_id(),
        f'{SIM_CACHE}.json',
        temp.name
      )
    except ClientError:
      return {}
    if os.path.exists(temp.name):
      with open(temp.name, 'rb') as tf:
        return json.load(tf)

def upload_sim_cache(sim_cache: dict):
  s3.upload_string(
    cache_task_id(),
    json.dumps(sim_cache),
    'sim_cache.json'
  )

def update_sim_cache(update: dict):
  sim_cache = download_sim_cache()
  sim_cache.update(update)
  upload_sim_cache(sim_cache)

def build_sim_cache():
  sim_cache = download_sim_cache()
  if 'duplicates' not in sim_cache:
    sim_cache['duplicates']=[]
  for folder in Folder.list():
    tasks = folder.list_tasks()
    if tasks is None:
      print("no tasks in folder")
      continue
    i=0
    for task in tqdm(tasks):
      id = task.task_id
      if not (i+1)%10: #save progress every x steps
        upload_sim_cache(sim_cache)
        i+=1
      if id in sim_cache.values() or id in sim_cache['duplicates']:
        #print("in cache")
        continue
      try:
        if task.get_simulation() is not None:
          key = hash(task.simulation.json())
          if key in sim_cache:
            sim_cache['duplicates'].append(id)
            print('duplicate_found')
          else:
            sim_cache[key]=id
            i+=1
      except ClientError:
        pass
  upload_sim_cache(sim_cache)
  return sim_cache
