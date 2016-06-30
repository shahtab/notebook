"""Tornado handlers for the contents web service.

Preliminary documentation at https://github.com/ipython/ipython/wiki/IPEP-27%3A-Contents-Service
"""

# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

import json

from tornado import gen, web

from notebook.utils import url_path_join, url_escape
from jupyter_client.jsonutil import date_default
from functools import reduce

from notebook.base.handlers import (
     APIHandler, json_errors, path_regex
)


def sort_key(model):
    """key function for case-insensitive sort by name and type"""
    iname = model['name'].lower()
    type_key = {
        'directory' : '0',
        'notebook'  : '1',
        'file'      : '2',
    }.get(model['type'], '9')
    return u'%s%s' % (type_key, iname)


def validate_model(model, expect_content):
    """
    Validate a model returned by a ContentsManager method.

    If expect_content is True, then we expect non-null entries for 'content'
    and 'format'.
    """
    required_keys = {
        "name",
        "path",
        "type",
        "writable",
        "created",
        "last_modified",
        "mimetype",
        "content",
        "format",
    }
    missing = required_keys - set(model.keys())
    if missing:
        raise web.HTTPError(
            500,
            u"Missing Model Keys: {missing}".format(missing=missing),
        )

    maybe_none_keys = ['content', 'format']
    if expect_content:
        errors = [key for key in maybe_none_keys if model[key] is None]
        if errors:
            raise web.HTTPError(
                500,
                u"Keys unexpectedly None: {keys}".format(keys=errors),
            )
    else:
        errors = {
            key: model[key]
            for key in maybe_none_keys
            if model[key] is not None
        }
        if errors:
            raise web.HTTPError(
                500,
                u"Keys unexpectedly not None: {keys}".format(keys=errors),
            )


class BeakerLabContentsHandler(APIHandler):
    def location_url(self, path):
        """Return the full URL location of a file.

        Parameters
        ----------
        path : unicode
            The API path of the file, such as "foo/bar.txt".
        """
        return url_path_join(
            self.base_url, 'api', 'contents', url_escape(path)
        )

    def _finish_model(self, model, location=True):
        """Finish a JSON request with a model, setting relevant headers, etc."""
        if location:
            location = self.location_url(model['path'])
            self.set_header('Location', location)
        self.set_header('Last-Modified', model['last_modified'])
        self.set_header('Content-Type', 'application/json')
        self.finish(json.dumps(model, default=date_default))

    @gen.coroutine
    def _upload(self, model, path):
        """Handle upload of a new file to path"""
        self.log.info(u"Uploading file to %s", path)
        model = yield gen.maybe_future(self.contents_manager.new(model, path))
        self.set_status(201)
        validate_model(model, expect_content=False)
        self._finish_model(model)

    @gen.coroutine
    def _save(self, model, path):
        """Save an existing file."""
        self.log.info(u"Saving file at %s", path)
        model = yield gen.maybe_future(self.contents_manager.save(model, path))
        validate_model(model, expect_content=False)
        self._finish_model(model)

    @web.authenticated
    @json_errors
    @gen.coroutine
    def put(self, path=''):
        """Saves the file in the location specified by name and path.

        PUT /api/contents/path/Name.ipynb
          Save notebook at ``path/Name.ipynb``. Notebook structure is specified
          in `content` key of JSON request body. If content is not specified,
          raise an error.
        """
        model = self.get_json_body()
        #TODO: save to a project dir
        #TODO: allow only notebooks
        if model:
            if model.get('copy_from'):
                raise web.HTTPError(403, "Copying not supported")
            exists = yield gen.maybe_future(self.contents_manager.file_exists(path))
            if exists:
                yield gen.maybe_future(self._save(model, path))
            else:
                yield gen.maybe_future(self._upload(model, path))
        else:
            raise web.HTTPError(500, "Cannot load an empty notebook")


class BeakerLabSessionHandler(APIHandler):

    def _get_from_dict(self, dict, key_path):
        return reduce(lambda d, k: d[k], key_path, dict)

    @web.authenticated
    @json_errors
    @gen.coroutine
    def get(self, session_id):
        # Returns the JSON model for a single session
        sm = self.session_manager
        model = yield gen.maybe_future(sm.get_session(session_id=session_id))
        self.finish(json.dumps(model, default=date_default))

    @web.authenticated
    @json_errors
    @gen.coroutine
    def delete(self, path):
        # Deletes the session with given path
        sm = self.session_manager
        sessions = yield gen.maybe_future(sm.list_sessions())
        key_path = ["notebook", "path"]
        nb_name = path.strip('/')
        session_id = None
        for session in sessions:
            nb_path = self._get_from_dict(session, key_path)
            if nb_path == nb_name:
                session_id = session["id"]
        if not (session_id is None):
            try:
                yield gen.maybe_future(sm.delete_session(session_id))
                exists = yield gen.maybe_future(self.contents_manager.file_exists(path))
                if exists:
                    cm = self.contents_manager
                    self.log.warning('Deleting %s', path)
                    yield gen.maybe_future(cm.delete(path))
                else:
                    self.log.warning('Path %s does not exist', path)
            except KeyError:
                # the kernel was deleted but the session wasn't!
                raise web.HTTPError(410, "Kernel deleted before session")
        self.set_status(204)
        self.finish()


class BeakerLabStatusHandler(APIHandler):

    @web.authenticated
    @json_errors
    @gen.coroutine
    def get(self, container_uuid):
        model = {"container_uuid": container_uuid}
        self.set_status(200)
        self.finish(json.dumps(model, default=date_default))



#-----------------------------------------------------------------------------
# URL to handler mappings
#-----------------------------------------------------------------------------

_session_id_regex = r"(?P<session_id>\w+-\w+-\w+-\w+-\w+)"
_uuid_regex = r"(?P<container_uuid>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"

default_handlers = [
    (r"beakerlab/api/sessions%s" % path_regex, BeakerLabSessionHandler),
    (r"status" , BeakerLabStatusHandler),
    (r"beakerlab/api/contents%s" % path_regex, BeakerLabContentsHandler)
]
