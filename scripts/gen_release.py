#!/usr/bin/env python3
"""Script to create Github releases."""

import hashlib
import json
import logging
import os
import subprocess
import sys
import zipfile

from third_party.python import colorlog, requests
from third_party.python.absl import app, flags

logging.root.handlers[0].setFormatter(colorlog.ColoredFormatter('%(log_color)s%(levelname)s: %(message)s'))


flags.DEFINE_string('github_token', os.environ.get('GITHUB_TOKEN'), 'Github API token')
flags.DEFINE_bool('dry_run', False, "Don't actually do the release, just print it.")
flags.mark_flag_as_required('github_token')
FLAGS = flags.FLAGS


class ReleaseGen:

    def __init__(self, github_token:str, dry_run:bool=False):
        self.url = 'https://api.github.com'
        self.releases_url = self.url + '/repos/thought-machine/please-servers/releases'
        self.upload_url = self.releases_url.replace('api.', 'uploads.') + '/<id>/assets?name='
        self.session = requests.Session()
        self.session.verify = '/etc/ssl/certs/ca-certificates.crt'
        if not dry_run:
            self.session.headers.update({
                'Accept': 'application/vnd.github.v3+json',
                'Authorization': 'token ' + github_token,
            })
        self.version = self.read_file('VERSION').strip()
        self.version_name = 'Version ' + self.version

    def needs_release(self):
        """Returns true if the current version is not yet released to Github."""
        url = self.releases_url + '/tags/v' + self.version
        logging.info('Checking %s for release...', url)
        response = self.session.get(url)
        return response.status_code == 404

    def release(self):
        """Submits a new release to Github."""
        data = {
            'tag_name': 'v' + self.version,
            'target_commitish': os.environ.get('CIRCLE_SHA1'),
            'name': 'v' + self.version,
            'body': '',
            'prerelease': False,
            'draft': False,
        }
        if FLAGS.dry_run:
            logging.info('Would post the following to Github: %s', json.dumps(data, indent=4))
            return
        logging.info('Creating release: %s',  json.dumps(data, indent=4))
        response = self.session.post(self.releases_url, json=data)
        response.raise_for_status()
        data = response.json()
        self.upload_url = data['upload_url'].replace('{?name,label}', '?name=')
        logging.info('Release id %s created', data['id'])

    def upload(self, artifact:str):
        """Uploads the given artifact to the new release."""
        filename = os.path.basename(artifact)
        content_type = 'application/octet-stream'
        url = self.upload_url + filename
        if FLAGS.dry_run:
            logging.info('Would upload %s to %s as %s', filename, url, content_type)
            return
        logging.info('Uploading %s to %s as %s', filename, url, content_type)
        with open(artifact, 'rb') as f:
            self.session.headers.update({'Content-Type': content_type})
            response = self.session.post(url, data=f)
            response.raise_for_status()
        print('%s uploaded' % filename)

    def read_file(self, filename):
        """Read a file from the .pex."""
        with zipfile.ZipFile(sys.argv[0]) as zf:
            return zf.read(filename).decode('utf-8')


def main(argv):
    r = ReleaseGen(FLAGS.github_token, dry_run=FLAGS.dry_run)
    if not r.needs_release():
        logging.info('Current version has already been released, nothing to be done!')
        return
    r.release()
    for artifact in argv[1:]:
        r.upload(artifact)


if __name__ == '__main__':
    app.run(main)
