from sqlalchemy.exc import IntegrityError

from redash import models
from redash.apis.handlers.base import (BaseResource,
                                       get_object_or_404)
from redash.permissions import require_access, view_only

class FolderStructureResource(BaseResource):
    def get(self, structure_id):
        folder = get_object_or_404(models.FolderStructure.get_by_id,
                                    structure_id)
        return folder.to_dict()

    def delete(self, structure_id):
        folder = get_object_or_404(models.QuerySnippet.get_by_id, structure_id)
        models.db.session.delete(folder)
        models.db.session.commit()

class FolderStructureListResource(BaseResource):
    def get(self):
        return [folder.to_dict() for folder in
                models.FolderStructure.all()]

    def post(self):
        req = request.get_json(True)
        require_fields(req, ('parent_id', 'catalog'))

        folder = models.FolderStructure(
            id=req['id'],
            parent_id=req['parent_id'],
            name=req['name'] if req['name'] else "New Folder",
            catalog=req['catalog']
        )

        models.db.session.add(folder)
        models.db.session.commit()

        return folder.to_dict()