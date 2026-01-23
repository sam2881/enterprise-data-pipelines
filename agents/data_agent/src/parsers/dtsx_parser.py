"""
DTSX Parser - Parse SSIS packages for migration to modern pipelines.

WHY: Enterprise migrations from SSIS to cloud-native pipelines require
     understanding the existing package structure, source-to-target mappings,
     and transformation logic.

HOW: Parse DTSX XML structure, extract:
     - Connection Managers (OLE DB, Flat File, etc.)
     - Data Flow Tasks (sources, transforms, destinations)
     - Execute SQL Tasks
     - Variables and Parameters
     - Precedence Constraints (task dependencies)

References:
- SSIS Package Structure: https://docs.microsoft.com/en-us/sql/integration-services/
- GenAI SSIS Migration: https://medium.com/dbsql-sme-engineering/genai-assisted-etl-migration-ssis-6677e2931606
"""

import re
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import xml.etree.ElementTree as ET

import structlog

logger = structlog.get_logger()


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class SSISConnection:
    """SSIS Connection Manager definition."""
    name: str
    connection_type: str  # OLE_DB, FLAT_FILE, ADO_NET, EXCEL, etc.
    connection_string: str
    provider: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    file_path: Optional[str] = None


@dataclass
class SSISColumn:
    """Column definition from SSIS component."""
    name: str
    data_type: str
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    code_page: Optional[int] = None


@dataclass
class SSISSource:
    """SSIS Source component."""
    name: str
    source_type: str  # OLE_DB, FLAT_FILE, EXCEL, ADO_NET
    connection_name: Optional[str] = None
    sql_command: Optional[str] = None
    table_name: Optional[str] = None
    columns: List[SSISColumn] = field(default_factory=list)


@dataclass
class SSISTransform:
    """SSIS Transformation component."""
    name: str
    transform_type: str  # DERIVED_COLUMN, LOOKUP, CONDITIONAL_SPLIT, SORT, AGGREGATE, MERGE_JOIN
    config: Dict[str, Any] = field(default_factory=dict)
    input_columns: List[str] = field(default_factory=list)
    output_columns: List[SSISColumn] = field(default_factory=list)


@dataclass
class SSISDestination:
    """SSIS Destination component."""
    name: str
    destination_type: str
    connection_name: Optional[str] = None
    table_name: Optional[str] = None
    columns: List[SSISColumn] = field(default_factory=list)
    fast_load: bool = True


@dataclass
class SSISSQLTask:
    """SSIS Execute SQL Task."""
    name: str
    connection_name: Optional[str] = None
    sql_statement: Optional[str] = None
    result_set: Optional[str] = None  # None, SingleRow, Full


@dataclass
class SSISVariable:
    """SSIS Variable or Parameter."""
    name: str
    namespace: str  # User, System
    data_type: str
    value: Any
    expression: Optional[str] = None


@dataclass
class SSISPackage:
    """Complete SSIS Package structure."""
    name: str
    description: Optional[str] = None
    version: Optional[str] = None

    connections: List[SSISConnection] = field(default_factory=list)
    variables: List[SSISVariable] = field(default_factory=list)
    sources: List[SSISSource] = field(default_factory=list)
    transforms: List[SSISTransform] = field(default_factory=list)
    destinations: List[SSISDestination] = field(default_factory=list)
    sql_tasks: List[SSISSQLTask] = field(default_factory=list)

    # Task execution order
    task_order: List[str] = field(default_factory=list)


# =============================================================================
# PARSER
# =============================================================================

class DTSXParser:
    """
    Parse SSIS DTSX packages.

    Usage:
        parser = DTSXParser()
        package = parser.parse_file('/path/to/package.dtsx')

        # Or from GCS
        package = parser.parse_from_gcs('gs://bucket/path/to/package.dtsx')

        # Get migration-ready summary
        summary = parser.get_migration_summary(package)
    """

    # SSIS XML namespaces
    NAMESPACES = {
        'DTS': 'www.microsoft.com/SqlServer/Dts',
        'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask',
        'pipeline': 'www.microsoft.com/SqlServer/Dts/Pipeline/ComponentMetaData',
    }

    # Connection type mappings
    CONNECTION_TYPES = {
        'OLEDB': 'OLE_DB',
        'FLATFILE': 'FLAT_FILE',
        'ADO': 'ADO_NET',
        'EXCEL': 'EXCEL',
        'ODBC': 'ODBC',
    }

    # Component class ID to type mappings
    COMPONENT_TYPES = {
        'Microsoft.OLEDBSource': 'OLE_DB',
        'Microsoft.OLEDBDestination': 'OLE_DB_DEST',
        'Microsoft.FlatFileSource': 'FLAT_FILE',
        'Microsoft.FlatFileDestination': 'FLAT_FILE_DEST',
        'Microsoft.DerivedColumn': 'DERIVED_COLUMN',
        'Microsoft.Lookup': 'LOOKUP',
        'Microsoft.ConditionalSplit': 'CONDITIONAL_SPLIT',
        'Microsoft.Sort': 'SORT',
        'Microsoft.Aggregate': 'AGGREGATE',
        'Microsoft.MergeJoin': 'MERGE_JOIN',
        'Microsoft.UnionAll': 'UNION_ALL',
        'Microsoft.DataConversion': 'DATA_CONVERSION',
        'Microsoft.RowCount': 'ROW_COUNT',
    }

    def __init__(self):
        self.logger = logger.bind(component="dtsx_parser")

    def parse_file(self, file_path: str) -> SSISPackage:
        """Parse DTSX file from local filesystem."""
        self.logger.info("Parsing DTSX file", path=file_path)

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        return self._parse_content(content, Path(file_path).stem)

    def parse_from_gcs(self, gcs_path: str, project_id: str = None) -> SSISPackage:
        """Parse DTSX file from Google Cloud Storage."""
        from google.cloud import storage

        self.logger.info("Parsing DTSX from GCS", path=gcs_path)

        # Parse GCS path
        path_parts = gcs_path.replace('gs://', '').split('/')
        bucket_name = path_parts[0]
        blob_path = '/'.join(path_parts[1:])

        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        content = blob.download_as_string().decode('utf-8')
        package_name = Path(blob_path).stem

        return self._parse_content(content, package_name)

    def parse_from_bytes(self, content: bytes, package_name: str = 'Unknown') -> SSISPackage:
        """Parse DTSX from bytes content."""
        return self._parse_content(content.decode('utf-8'), package_name)

    def _parse_content(self, content: str, package_name: str) -> SSISPackage:
        """Parse DTSX XML content."""
        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            self.logger.error("Failed to parse DTSX XML", error=str(e))
            raise ValueError(f"Invalid DTSX XML: {e}")

        package = SSISPackage(name=package_name)

        # Parse package metadata
        package.description = self._get_property(root, 'Description')
        package.version = root.get(f'{{{self.NAMESPACES["DTS"]}}}VersionGUID', '')

        # Parse components
        package.connections = self._parse_connections(root)
        package.variables = self._parse_variables(root)
        package.sql_tasks = self._parse_sql_tasks(root)

        # Parse Data Flow Tasks
        sources, transforms, destinations = self._parse_data_flows(root)
        package.sources = sources
        package.transforms = transforms
        package.destinations = destinations

        # Parse execution order
        package.task_order = self._parse_precedence_constraints(root)

        self.logger.info(
            "DTSX parsed successfully",
            connections=len(package.connections),
            sources=len(package.sources),
            transforms=len(package.transforms),
            destinations=len(package.destinations),
        )

        return package

    def _get_property(self, element: ET.Element, prop_name: str) -> Optional[str]:
        """Get DTS property value from element."""
        for prop in element.findall(f'.//{{{self.NAMESPACES["DTS"]}}}Property', self.NAMESPACES):
            if prop.get(f'{{{self.NAMESPACES["DTS"]}}}Name') == prop_name:
                return prop.text
        return None

    def _parse_connections(self, root: ET.Element) -> List[SSISConnection]:
        """Parse Connection Managers."""
        connections = []

        for conn_elem in root.findall(f'.//{{{self.NAMESPACES["DTS"]}}}ConnectionManager', self.NAMESPACES):
            name = conn_elem.get(f'{{{self.NAMESPACES["DTS"]}}}ObjectName', 'Unknown')
            conn_string = self._get_property(conn_elem, 'ConnectionString') or ''

            # Determine connection type
            creation_name = conn_elem.get(f'{{{self.NAMESPACES["DTS"]}}}CreationName', '')
            conn_type = 'UNKNOWN'
            for key, val in self.CONNECTION_TYPES.items():
                if key in creation_name.upper():
                    conn_type = val
                    break

            # Parse connection string components
            conn_parts = self._parse_connection_string(conn_string)

            connections.append(SSISConnection(
                name=name,
                connection_type=conn_type,
                connection_string=conn_string,
                provider=conn_parts.get('provider'),
                server=conn_parts.get('server') or conn_parts.get('data source'),
                database=conn_parts.get('database') or conn_parts.get('initial catalog'),
                file_path=conn_parts.get('connectionstring'),
            ))

        return connections

    def _parse_connection_string(self, conn_str: str) -> Dict[str, str]:
        """Parse connection string into key-value pairs."""
        result = {}
        for part in conn_str.split(';'):
            if '=' in part:
                key, value = part.split('=', 1)
                result[key.strip().lower()] = value.strip()
        return result

    def _parse_variables(self, root: ET.Element) -> List[SSISVariable]:
        """Parse package variables and parameters."""
        variables = []

        for var_elem in root.findall(f'.//{{{self.NAMESPACES["DTS"]}}}Variable', self.NAMESPACES):
            name = var_elem.get(f'{{{self.NAMESPACES["DTS"]}}}ObjectName', '')
            namespace = var_elem.get(f'{{{self.NAMESPACES["DTS"]}}}Namespace', 'User')

            data_type = self._get_property(var_elem, 'DataType') or 'String'
            value = self._get_property(var_elem, 'Value')
            expression = self._get_property(var_elem, 'Expression')

            variables.append(SSISVariable(
                name=name,
                namespace=namespace,
                data_type=data_type,
                value=value,
                expression=expression,
            ))

        return variables

    def _parse_sql_tasks(self, root: ET.Element) -> List[SSISSQLTask]:
        """Parse Execute SQL Tasks."""
        tasks = []

        # Find all ExecuteSQLTask executables
        for exec_elem in root.findall(f'.//{{{self.NAMESPACES["DTS"]}}}Executable', self.NAMESPACES):
            exec_type = exec_elem.get(f'{{{self.NAMESPACES["DTS"]}}}ExecutableType', '')

            if 'ExecuteSQLTask' in exec_type:
                name = exec_elem.get(f'{{{self.NAMESPACES["DTS"]}}}ObjectName', 'SQL Task')

                # Get SQL statement
                sql = None
                for prop in exec_elem.findall(f'.//{{{self.NAMESPACES["DTS"]}}}Property', self.NAMESPACES):
                    prop_name = prop.get(f'{{{self.NAMESPACES["DTS"]}}}Name', '')
                    if prop_name == 'SqlStatementSource':
                        sql = prop.text

                # Get connection
                conn_name = None
                for obj_data in exec_elem.findall(f'.//{{{self.NAMESPACES["DTS"]}}}ObjectData', self.NAMESPACES):
                    sql_task = obj_data.find(f'.//{{{self.NAMESPACES["SQLTask"]}}}SqlTaskData', self.NAMESPACES)
                    if sql_task is not None:
                        conn_name = sql_task.get(f'{{{self.NAMESPACES["SQLTask"]}}}Connection')

                tasks.append(SSISSQLTask(
                    name=name,
                    connection_name=conn_name,
                    sql_statement=sql,
                ))

        return tasks

    def _parse_data_flows(self, root: ET.Element) -> Tuple[List[SSISSource], List[SSISTransform], List[SSISDestination]]:
        """Parse Data Flow Task components."""
        sources = []
        transforms = []
        destinations = []

        # Find Data Flow Tasks (Pipeline executables)
        for exec_elem in root.findall(f'.//{{{self.NAMESPACES["DTS"]}}}Executable', self.NAMESPACES):
            exec_type = exec_elem.get(f'{{{self.NAMESPACES["DTS"]}}}ExecutableType', '')

            if 'Pipeline' in exec_type or 'SSIS.Pipeline' in exec_type:
                # Parse pipeline components
                for component in exec_elem.findall('.//component'):
                    comp_class = component.get('componentClassID', '')
                    comp_name = component.get('name', 'Unknown')

                    # Determine component type
                    comp_type = 'UNKNOWN'
                    for class_id, type_name in self.COMPONENT_TYPES.items():
                        if class_id in comp_class:
                            comp_type = type_name
                            break

                    # Parse based on component type
                    if 'Source' in comp_class or comp_type in ['OLE_DB', 'FLAT_FILE']:
                        source = self._parse_source_component(component, comp_name, comp_type)
                        sources.append(source)

                    elif 'Destination' in comp_class or comp_type in ['OLE_DB_DEST', 'FLAT_FILE_DEST']:
                        dest = self._parse_destination_component(component, comp_name, comp_type)
                        destinations.append(dest)

                    else:
                        transform = self._parse_transform_component(component, comp_name, comp_type)
                        transforms.append(transform)

        return sources, transforms, destinations

    def _parse_source_component(self, component: ET.Element, name: str, source_type: str) -> SSISSource:
        """Parse source component details."""
        source = SSISSource(name=name, source_type=source_type)

        # Get SQL command or table name
        for prop in component.findall('.//property'):
            prop_name = prop.get('name', '')
            if prop_name == 'SqlCommand':
                source.sql_command = prop.text
            elif prop_name == 'OpenRowset':
                source.table_name = prop.text

        # Get output columns
        for col in component.findall('.//outputColumn'):
            source.columns.append(SSISColumn(
                name=col.get('name', ''),
                data_type=col.get('dataType', 'DT_STR'),
                length=int(col.get('length', 0)) or None,
                precision=int(col.get('precision', 0)) or None,
                scale=int(col.get('scale', 0)) or None,
            ))

        return source

    def _parse_destination_component(self, component: ET.Element, name: str, dest_type: str) -> SSISDestination:
        """Parse destination component details."""
        dest = SSISDestination(name=name, destination_type=dest_type)

        # Get table name
        for prop in component.findall('.//property'):
            prop_name = prop.get('name', '')
            if prop_name == 'OpenRowset':
                dest.table_name = prop.text
            elif prop_name == 'FastLoadOptions':
                dest.fast_load = True

        return dest

    def _parse_transform_component(self, component: ET.Element, name: str, transform_type: str) -> SSISTransform:
        """Parse transformation component details."""
        transform = SSISTransform(name=name, transform_type=transform_type)

        if transform_type == 'DERIVED_COLUMN':
            # Parse derived column expressions
            transform.config['columns'] = []
            for col in component.findall('.//outputColumn'):
                expr_prop = col.find('.//property[@name="Expression"]')
                transform.config['columns'].append({
                    'name': col.get('name', ''),
                    'expression': expr_prop.text if expr_prop is not None else '',
                })

        elif transform_type == 'LOOKUP':
            # Parse lookup configuration
            for prop in component.findall('.//property'):
                prop_name = prop.get('name', '')
                if prop_name == 'SqlCommand':
                    transform.config['lookup_query'] = prop.text
                elif prop_name == 'NoMatchBehavior':
                    transform.config['no_match_behavior'] = prop.text

        elif transform_type == 'CONDITIONAL_SPLIT':
            # Parse conditions
            transform.config['conditions'] = []
            for output in component.findall('.//output'):
                if output.get('isErrorOut') != 'true':
                    transform.config['conditions'].append({
                        'name': output.get('name', ''),
                        'expression': output.get('expression', ''),
                    })

        elif transform_type == 'AGGREGATE':
            # Parse aggregations
            transform.config['aggregations'] = []
            for col in component.findall('.//outputColumn'):
                agg_type = col.find('.//property[@name="AggregationType"]')
                transform.config['aggregations'].append({
                    'column': col.get('name', ''),
                    'type': agg_type.text if agg_type is not None else 'GroupBy',
                })

        return transform

    def _parse_precedence_constraints(self, root: ET.Element) -> List[str]:
        """Parse task execution order from precedence constraints."""
        # Build dependency graph
        dependencies = {}
        task_names = set()

        for constraint in root.findall(f'.//{{{self.NAMESPACES["DTS"]}}}PrecedenceConstraint', self.NAMESPACES):
            from_task = constraint.get(f'{{{self.NAMESPACES["DTS"]}}}From', '')
            to_task = constraint.get(f'{{{self.NAMESPACES["DTS"]}}}To', '')

            if to_task not in dependencies:
                dependencies[to_task] = []
            dependencies[to_task].append(from_task)

            task_names.add(from_task)
            task_names.add(to_task)

        # Topological sort
        order = []
        visited = set()

        def visit(task):
            if task in visited:
                return
            visited.add(task)
            for dep in dependencies.get(task, []):
                visit(dep)
            order.append(task)

        for task in task_names:
            visit(task)

        return order

    def get_migration_summary(self, package: SSISPackage) -> Dict[str, Any]:
        """
        Generate migration-ready summary for LLM context.

        Returns a structured summary that can be passed to LLM for
        generating modern pipeline code.
        """
        return {
            'package_name': package.name,
            'description': package.description,

            'connections': [
                {
                    'name': c.name,
                    'type': c.connection_type,
                    'server': c.server,
                    'database': c.database,
                }
                for c in package.connections
            ],

            'sources': [
                {
                    'name': s.name,
                    'type': s.source_type,
                    'table': s.table_name,
                    'query': s.sql_command[:500] if s.sql_command else None,
                    'columns': [{'name': c.name, 'type': c.data_type} for c in s.columns],
                }
                for s in package.sources
            ],

            'transformations': [
                {
                    'name': t.name,
                    'type': t.transform_type,
                    'config': t.config,
                }
                for t in package.transforms
            ],

            'destinations': [
                {
                    'name': d.name,
                    'type': d.destination_type,
                    'table': d.table_name,
                }
                for d in package.destinations
            ],

            'sql_tasks': [
                {
                    'name': t.name,
                    'sql': t.sql_statement[:500] if t.sql_statement else None,
                }
                for t in package.sql_tasks
            ],

            'variables': [
                {'name': f"{v.namespace}::{v.name}", 'value': v.value}
                for v in package.variables if v.namespace == 'User'
            ],

            'execution_order': package.task_order,
        }

    def generate_migration_prompt(self, package: SSISPackage) -> str:
        """
        Generate LLM prompt for SSIS to Airflow/PySpark migration.
        """
        summary = self.get_migration_summary(package)

        prompt = f"""You are an expert data engineer migrating SSIS packages to modern cloud-native pipelines.

## SSIS Package: {summary['package_name']}

### Connections:
{json.dumps(summary['connections'], indent=2)}

### Data Sources:
{json.dumps(summary['sources'], indent=2)}

### Transformations:
{json.dumps(summary['transformations'], indent=2)}

### Destinations:
{json.dumps(summary['destinations'], indent=2)}

### SQL Tasks:
{json.dumps(summary['sql_tasks'], indent=2)}

### Task Execution Order:
{summary['execution_order']}

## TASK:
Generate a modern Airflow DAG with PySpark tasks that replicates this SSIS package functionality.
The DAG should:
1. Use TaskFlow API (@task decorator)
2. Implement medallion architecture (Bronze -> Silver -> Gold)
3. Include data quality checks
4. Load final data to BigQuery
5. Include proper error handling and logging

Provide the complete Python code for the DAG.
"""
        return prompt
