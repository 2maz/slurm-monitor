Command Line Interface
========================

The command line interface offers a number of workflow simplifications that are encapsulated
in sub-commands:

#auto-deploy,db,import,listen,listen-ui,probe,query,restapi,spec,system-info,test

**auto-deploy**
    TBD

**db**
    Compare and optionally update the running database with the (sqlalchemy)-defined schemas - table, columns, and indexes

**listen**
    Connect the kafka-broker and the database, i.e., attach to cluster-specific topic and feed the database with the messages

**listen-ui**
    Monitor the listener (when started with listen and the corresponding 'ui' options

**restapi**
    Start the restapi endpoints

**probe**
    TBD

**query**
    TDB

**spec**
    TDB

**system-info**
    TDB

**test**
    TDB

restapi
--------

.. literalinclude:: ./examples/slurm-monitor-restapi-help.txt
  :language: none


Allow to start the restapi with a configuration defined in an .env file

Examples
^^^^^^^^

.. highlight:: python

::

    slurm-monitor restapi --env-file .dev.env


