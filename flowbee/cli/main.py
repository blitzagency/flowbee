import os
from circus import get_arbiter
from circus.pidfile import Pidfile
import click


@click.command()
@click.version_option(version='0.0.1')
@click.option('--type', required=True, type=click.Choice(["worker", "decider", "both"]))
@click.option('--workers', "-w", default=1, help="Number of workers.")
@click.option('--workflow', "-f", required=True, help="Python path for workflow ex: foo.bar.Baz")
@click.option('--pidfile', "-p", default=None, help="PID file")
@click.option('--sync/--no-sync', default=True, help="Should AWS SWF Resources be created?")
@click.option('--environ', "-e", default=None, help="Enviroment variables to load")
@click.option('--log-config', default=None, help="Standard python logging configuration formatted file")
@click.option('--log-level', default="INFO", help="Logging level. Specifying a log config negates this option")
def main(type, workers, workflow, pidfile, sync, environ, log_config, log_level):
    log_level = log_level.upper()

    types = []
    if type == "both":
        types = ["worker", "decider"]
    else:
        types = [type]

    if pidfile is None:
        pidfile = "/tmp/flowbee_{}.pid".format(type)

    apps = build_apps(
        types,
        workers=workers,
        workflow=workflow,
        sync=sync,
        environ=environ,
        log_config=log_config,
        log_level=log_level
    )

    arbiter = get_arbiter(
        apps,
        pidfile=pidfile,
        loglevel=log_level
    )

    pidfile = Pidfile(arbiter.pidfile)
    pidfile.create(os.getpid())

    try:
        arbiter.start()
    finally:
        pidfile.unlink()
        arbiter.stop()


def build_apps(types, workers, workflow, sync, environ, log_config, log_level, **kw):
    apps = []
    for type in types:
        app = build_app(
            type,
            workers=workers,
            workflow=workflow,
            sync=sync,
            environ=environ,
            log_config=log_config,
            log_level=log_level,
            **kw
        )
        apps.append(app)
    return apps


def build_app(type, workers, workflow, sync, environ, log_config, log_level, **kw):
    cmd = [
        "python",
        "-m", "flowbee.cli.{}".format(type),
        "-f", workflow,
    ]

    if environ:
        cmd.extend(["-e", environ])

    if sync:
        cmd.extend(["--sync"])
    else:
        cmd.extend(["--no-sync"])

    if log_config:
        cmd.extend(["--log-config", log_config])

    if log_level:
        cmd.extend(["--log-level", log_level])

    app = {
        "cmd": " ".join(cmd),
        "numprocesses": workers,
        "send_hup": True,
        "copy_env": True,
        "copy_path": True
    }

    virtualenv = os.getenv("VIRTUAL_ENV")

    if virtualenv:
        app.update({
            "virtualenv": virtualenv,
        })

    app.update(kw)

    return app

if __name__ == "__main__":
    main()
