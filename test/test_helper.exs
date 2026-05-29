:ok = Logger.App.stop()

Application.ensure_all_started(:hmnt)

:ok = LocalCluster.start()

ExUnit.start(exclude: [:skip])
