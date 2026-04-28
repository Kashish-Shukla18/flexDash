import json
import pandas as pd
from io import StringIO
import re

from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.cache import cache
from django.db import connection
from django.conf import settings

CACHE_TTL = getattr(settings, 'CACHE_TTL', 1800)
SAFE_NAME_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


CHART_COLORS = [
    'rgba(99,102,241,{a})', 'rgba(16,185,129,{a})', 'rgba(245,158,11,{a})',
    'rgba(239,68,68,{a})', 'rgba(59,130,246,{a})', 'rgba(168,85,247,{a})',
    'rgba(20,184,166,{a})', 'rgba(251,146,60,{a})', 'rgba(236,72,153,{a})',
    'rgba(132,204,22,{a})',
]


def _get_color(i, alpha=0.8):
    return CHART_COLORS[i % len(CHART_COLORS)].format(a=alpha)


def _safe_session(request):
    if not request.session.session_key:
        request.session.create()
    return request.session.session_key
    
def home(request):
    _safe_session(request)
    return render(request, 'index.html')


@csrf_exempt
def list_tables(request):
    """Return user-visible tables from the public schema."""
    if request.method != 'GET':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
            rows = cur.fetchall()

        ignored = {
            'django_migrations',
            'django_admin_log',
            'django_content_type',
            'auth_permission',
            'auth_group',
            'auth_group_permissions',
            'auth_user',
            'auth_user_groups',
            'auth_user_user_permissions',
            'django_session',
        }
        tables = [r[0] for r in rows if r[0] not in ignored]
        return JsonResponse({'tables': tables, 'success': True})
    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)


@csrf_exempt
def load_table_data(request):
    """Load data from a selected DB table and cache it for dashboard usage."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        body = json.loads(request.body)
        table_name = (body.get('table_name') or '').strip()

        if not table_name or not SAFE_NAME_RE.match(table_name):
            return JsonResponse(
                {'error': 'Invalid table name. Use letters, digits and underscores only.'},
                status=400,
            )

        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                [table_name],
            )
            if cur.fetchone() is None:
                return JsonResponse({'error': 'Table not found.'}, status=404)

            cur.execute(f'SELECT * FROM "{table_name}"')
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]

        df = pd.DataFrame(rows, columns=cols)
        if df.empty:
            return JsonResponse({'error': 'Selected table has no rows.'}, status=400)

        session_key = _safe_session(request)
        cache_key = f'dashboard_{session_key}'
        df.columns = [str(c).strip() for c in df.columns]

        cache.set(
            cache_key,
            {
                'data': df.to_json(orient='records', date_format='iso'),
                'columns': list(df.columns),
                'filename': f'table:{table_name}',
            },
            CACHE_TTL,
        )

        numeric_cols = df.select_dtypes(include='number').columns.tolist()
        return JsonResponse(
            {
                'cache_key': cache_key,
                'columns': list(df.columns),
                'numeric_columns': numeric_cols,
                'preview': df.head(5).fillna('').to_dict(orient='records'),
                'total_rows': len(df),
                'filename': f'table:{table_name}',
                'table_name': table_name,
            }
        )
    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)


@csrf_exempt
def upload_sheet(request):
    """Parse an uploaded CSV/Excel file and cache the data for this session."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    file = request.FILES.get('file')
    if not file:
        return JsonResponse({'error': 'No file uploaded'}, status=400)

    try:
        name = file.name.lower()
        if name.endswith('.csv'):
            df = pd.read_csv(file)
        elif name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(file)
        else:
            return JsonResponse(
                {'error': 'Unsupported format. Upload a CSV or Excel file.'}, status=400
            )

        if df.empty:
            return JsonResponse({'error': 'The uploaded file contains no data.'}, status=400)

        # Sanitise column names (strip whitespace)
        df.columns = [str(c).strip() for c in df.columns]

        session_key = _safe_session(request)
        cache_key = f'dashboard_{session_key}'

        cache.set(
            cache_key,
            {'data': df.to_json(orient='records'), 'columns': list(df.columns), 'filename': file.name},
            CACHE_TTL,
        )

        numeric_cols = df.select_dtypes(include='number').columns.tolist()

        return JsonResponse({
            'cache_key': cache_key,
            'columns': list(df.columns),
            'numeric_columns': numeric_cols,
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
            'preview': df.head(5).fillna('').to_dict(orient='records'),
            'total_rows': len(df),
            'filename': file.name,
        })

    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)


@csrf_exempt
def get_chart_data(request):
    """Return Chart.js-ready datasets for the selected columns and chart types."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        body = json.loads(request.body)
        cache_key = body.get('cache_key')
        x_col = body.get('x_column')
        y_cols = body.get('y_columns', [])
        chart_types = body.get('chart_types', [])
        time_column = body.get('time_column')
        time_start = body.get('time_start')
        time_end = body.get('time_end')

        if not cache_key or not x_col or not y_cols or not chart_types:
            return JsonResponse({'error': 'Missing required parameters.'}, status=400)

        cached = cache.get(cache_key)
        if not cached:
            return JsonResponse(
                {'error': 'Session data expired or not found. Please re-upload your file.'}, status=404
            )

        df = pd.read_json(StringIO(cached['data']))

        if time_column and time_column in df.columns and (time_start or time_end):
            ts = pd.to_datetime(df[time_column], errors='coerce', utc=True)
            mask = ts.notna()
            if time_start:
                start_dt = pd.to_datetime(time_start, errors='coerce', utc=True)
                if pd.notna(start_dt):
                    mask = mask & (ts >= start_dt)
            if time_end:
                end_dt = pd.to_datetime(time_end, errors='coerce', utc=True)
                if pd.notna(end_dt):
                    mask = mask & (ts <= end_dt)
            df = df[mask].copy()

        if df.empty:
            return JsonResponse({'error': 'No data in selected chart time range.'}, status=400)

        labels = df[x_col].astype(str).tolist()

        charts = []
        for chart_type in chart_types:
            is_pie_like = chart_type in ('pie', 'doughnut', 'polarArea')
            datasets = []

            for i, y_col in enumerate(y_cols):
                if y_col not in df.columns:
                    continue
                data_vals = df[y_col].fillna(0).tolist()
                border_color = _get_color(i, 1)
                fill_color = _get_color(i, 0.8)

                if is_pie_like:
                    bg_colors = [_get_color(j, 0.8) for j in range(len(data_vals))]
                    datasets.append({
                        'label': y_col,
                        'data': data_vals,
                        'backgroundColor': bg_colors,
                        'borderColor': [_get_color(j, 1) for j in range(len(data_vals))],
                        'borderWidth': 2,
                    })
                    break  # pie/doughnut only uses the first Y column naturally; others shown per-dataset
                else:
                    dataset = {
                        'label': y_col,
                        'data': data_vals,
                        'backgroundColor': fill_color if chart_type == 'bar' else _get_color(i, 0.15),
                        'borderColor': border_color,
                        'borderWidth': 2,
                        'fill': chart_type == 'area',
                        'tension': 0.4,
                        'pointRadius': 4,
                    }
                    datasets.append(dataset)

            actual_type = 'line' if chart_type == 'area' else chart_type
            charts.append({
                'type': actual_type,
                'display_type': chart_type,
                'labels': labels,
                'datasets': datasets,
                'title': f'{", ".join(y_cols)} vs {x_col}',
                'x_col': x_col,
                'y_cols': y_cols,
            })

        return JsonResponse({'charts': charts, 'success': True})

    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)


@csrf_exempt
def save_to_database(request):
    """Dynamically create a PostgreSQL table and insert the cached data."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        body = json.loads(request.body)
        cache_key = body.get('cache_key')
        raw_name = body.get('table_name', '').strip().lower()

        # Strict validation: only letters, digits, underscores
        if not raw_name or not SAFE_NAME_RE.match(raw_name):
            return JsonResponse(
                {'error': 'Table name must contain only letters, digits, or underscores.'}, status=400
            )
        if raw_name[0].isdigit():
            return JsonResponse({'error': 'Table name must not start with a digit.'}, status=400)

        cached = cache.get(cache_key)
        if not cached:
            return JsonResponse(
                {'error': 'Session data expired. Please re-upload your file.'}, status=404
            )

        df = pd.read_json(StringIO(cached['data']))

        def sql_type(dtype):
            s = str(dtype)
            if 'int' in s:
                return 'BIGINT'
            if 'float' in s:
                return 'DOUBLE PRECISION'
            return 'TEXT'

        col_defs = ', '.join(f'"{col}" {sql_type(df[col].dtype)}' for col in df.columns)

        with connection.cursor() as cur:
            cur.execute(
                f'CREATE TABLE IF NOT EXISTS "{raw_name}" (id SERIAL PRIMARY KEY, {col_defs})'
            )
            col_names = ', '.join(f'"{c}"' for c in df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            for _, row in df.iterrows():
                values = [None if pd.isna(v) else v for v in row]
                cur.execute(
                    f'INSERT INTO "{raw_name}" ({col_names}) VALUES ({placeholders})', values
                )

        return JsonResponse({
            'success': True,
            'table_name': raw_name,
            'rows_saved': len(df),
            'columns': list(df.columns),
        })

    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)


@csrf_exempt
def clear_cache(request):
    """Delete the cached dataset for this session."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        body = json.loads(request.body)
        cache_key = body.get('cache_key')
        if cache_key:
            cache.delete(cache_key)
        return JsonResponse({'success': True})
    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)


@csrf_exempt
def check_cache(request):
    """Return whether the cache key still holds data (used for TTL indicator)."""
    if request.method != 'POST':
        return JsonResponse({'error': 'Method not allowed'}, status=405)

    try:
        body = json.loads(request.body)
        cache_key = body.get('cache_key')
        alive = cache.get(cache_key) is not None
        return JsonResponse({'alive': alive})
    except Exception as exc:
        return JsonResponse({'error': str(exc)}, status=500)
