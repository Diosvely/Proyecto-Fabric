[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_estaciones_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_estaciones = let
  Origen = Lakehouse.Contents([]),
  #"Navegación 1" = Origen{[workspaceId = "0cd16d15-c661-4d9d-899a-376896ab25f0"]}[Data],
  #"Navegación 2" = #"Navegación 1"{[lakehouseId = "de76f0fe-6205-45e5-aa76-7b2c8e557725"]}[Data],
  #"Navegación 3" = #"Navegación 2"{[Id = "dim_estaciones", ItemKind = "View"]}[Data]
in
  #"Navegación 3";
shared dim_estaciones_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "0cd16d15-c661-4d9d-899a-376896ab25f0"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "4bde9b09-848b-4e90-abc3-6bfd35f2a1d9"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_estacionesOk", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
