from typing import IO
# Dict, Generator, Generic, List, TypeVar, Union
from .Api import Api
import secrets
from .env import *
from .Task import Task


class Client:
    def __init__(self,
                 api_key):
        self.api = Api(api_key=api_key, api_instance_url=LABELER_BASE_URL)

    def create_project(self, project):
        return self.api.post_request('create-project', body=project.to_dic())

    def create_class(self, project_id, _class):
        return self.api.post_request('create-class', body={**_class.to_dic(), "project_id": project_id})

    def create_classes(self, project_id, classes):
        return self.api.post_request('create-classes', body={"project_id": project_id,
                                                             "classes": [_class.to_dic() for _class in classes]})

    def edit_project(self, project, project_id):
        return self.api.put_request('edit-project', body=project.to_dic(), headers={"project_id": project_id})

    def get_all_projects(self):
        return self.api.get_request('all-projects')

    def get_project(self, project_id):
        return self.api.get_request('project', headers={'project_id': project_id})

    def get_class(self, project_id, class_id):
        return self.api.get_request('class', headers={'project_id': project_id}, params={'class_id': class_id,
                                                                                         })

    def get_all_classes(self, project_id):
        return self.api.get_request('classes',
                                    headers={'project_id': project_id})

    def cancel_task(self, project_id, task_id):
        return self.api.delete_request('cancel-task',
                                       headers={'project_id': project_id},
                                       params={'task_id': task_id})

    def get_task(self, project_id, task_id):
        return self.api.get_request('task',
                                    headers={'project_id': project_id},
                                    params={'task_id': task_id})

    def get_all_tasks(self, project_id, status):
        return self.api.get_request('tasks',
                                    headers={'project_id': project_id},
                                    params={'status': status})

    def get_tasks_count(self, project_id, status):
        return self.api.get_request('tasks-count',
                                    headers={'project_id': project_id},
                                    params={'status': status})

    def create_task(self,
                    project_id: str,
                    task: Task):
        if task.unique_id is None:
            task.set_id(secrets.token_hex(8))
        return self.api.post_request('create-task',
                                     headers={'project_id': project_id},
                                     body=task.to_dic())

    def all_labelers(self, project_id):
        return self.api.get_request('all-labelers', headers={'project_id': project_id})

    def add_labelers(self, project_id, emails):
        return self.api.post_request('add-labelers', headers={'project_id': project_id},
                                     body={"emails": emails})

    def cancel_labeler(self, project_id, email):
        return self.api.delete_request('cancel-labeler', headers={'project_id': project_id},
                                       params={"email": email})

    def import_file(self,
                    file_url: str):
        body = {
            'file_url': file_url
        }
        return self.api.post_request('file/import-file',
                                     headers={}, body=body)

    def upload_file(self, file_path):
        files = {
            "document":  ('Germany.png', open(file_path, "rb"), 'image/png')
        }
        return self.api.post_request('file/upload-file',
                                     files=files)

    def download_file(self, file_id: str):
        return self.api.get_request('file/download-file',
                                    params={'file-id': file_id})
