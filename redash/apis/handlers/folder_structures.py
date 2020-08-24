from flask import request
from funcy import project
from flask_restful import abort
from sqlalchemy.exc import IntegrityError

from redash import models

from redash.apis.handlers.base import (BaseResource, get_object_or_404,
                                       require_fields)
from redash.permissions import require_access, view_only

class FolderStructureResource(BaseResource):
    def get(self, structure_id):
        folder = get_object_or_404(models.FolderStructure.get_by_id,
                                    structure_id)
        return folder.to_dict()

    def delete(self, structure_id):
        folder = get_object_or_404(models.FolderStructure.get_by_id, structure_id)
        models.db.session.delete(folder)
        models.db.session.commit()
        return folder.to_dict()

    def post(self, structure_id):
        folder = get_object_or_404(models.FolderStructure.get_by_id,
                                    structure_id)
        req = request.get_json(True)
        if req and "name" in req:
            folder.update_name(req['name'])
        else:
            abort(400)
        return folder.to_dict()


class FolderStructureListResource(BaseResource):
    def get(self):
        return [folder.to_dict() for folder in
                models.FolderStructure.all()]

    def post(self):
        req = request.get_json(True)
        # require_fields(req, ('catalog'))

        if req and "parent_id" in req:
            folder = models.FolderStructure(
                parent_id=req['parent_id'],
                name=req['name'] if "name" in req else "New Folder",
                catalog=req['catalog']
            )
        else:
            folder = models.FolderStructure(
                name=req['name'] if "name" in req else "New Folder",
                catalog=req['catalog']
            )

        models.db.session.add(folder)
        models.db.session.commit()

        return folder.to_dict()