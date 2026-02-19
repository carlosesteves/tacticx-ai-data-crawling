"""
Mapping between football-data.co.uk league codes and Transfermarkt league codes.

This mapping is used to connect the abbreviated codes from football-data.co.uk
(e.g., 'E0', 'E1') with the Transfermarkt league codes stored in our database
(e.g., 'GB1', 'GB2').

The mapping includes tier information to help disambiguate teams with similar names
that play in different divisions of the same country.
"""

# Mapping format: football-data code -> (transfermarkt code, country, tier, full name)
FOOTBALL_DATA_TO_TM_LEAGUE_MAP = {
    # England
    'E0': ('GB1', 'England', 1, 'Premier League'),
    'E1': ('GB2', 'England', 2, 'Championship'),
    'E2': ('GB3', 'England', 3, 'League One'),
    'E3': ('GB4', 'England', 4, 'League Two'),
    'EC': ('GB5', 'England', 5, 'National League'),
    
    # Scotland
    'SC0': ('SC1', 'Scotland', 1, 'Premiership'),
    'SC1': ('SC2', 'Scotland', 2, 'Championship'),
    'SC2': ('SC3', 'Scotland', 3, 'League One'),
    'SC3': ('SC4', 'Scotland', 4, 'League Two'),
    
    # Germany
    'D1': ('L1', 'Germany', 1, 'Bundesliga'),
    'D2': ('L2', 'Germany', 2, '2. Bundesliga'),
    
    # Italy
    'I1': ('IT1', 'Italy', 1, 'Serie A'),
    'I2': ('IT2', 'Italy', 2, 'Serie B'),
    
    # Spain
    'SP1': ('ES1', 'Spain', 1, 'La Liga'),
    'SP2': ('ES2', 'Spain', 2, 'Segunda División'),
    
    # France
    'F1': ('FR1', 'France', 1, 'Ligue 1'),
    'F2': ('FR2', 'France', 2, 'Ligue 2'),
    
    # Netherlands
    'N1': ('NL1', 'Netherlands', 1, 'Eredivisie'),
    
    # Belgium
    'B1': ('BE1', 'Belgium', 1, 'First Division A'),
    
    # Portugal
    'P1': ('PO1', 'Portugal', 1, 'Primeira Liga'),
    
    # Turkey
    'T1': ('TR1', 'Turkey', 1, 'Süper Lig'),
    
    # Greece
    'G1': ('GR1', 'Greece', 1, 'Super League'),
    
    # Argentina
    'ARG': ('ARG1', 'Argentina', 1, 'Primera División'),
    
    # Austria
    'AUT': ('A1', 'Austria', 1, 'Bundesliga'),
    
    # Brazil
    'BRA': ('BRA1', 'Brazil', 1, 'Série A'),
    
    # China
    'CHN': ('CSL', 'China', 1, 'Super League'),
    
    # Denmark
    'DNK': ('DK1', 'Denmark', 1, 'Superliga'),
    
    # Finland
    'FIN': ('FI1', 'Finland', 1, 'Veikkausliiga'),
    
    # Ireland
    'IRL': ('IR1', 'Ireland', 1, 'Premier Division'),
    
    # Japan
    'JPN': ('JAP1', 'Japan', 1, 'J1 League'),
    
    # Mexico
    'MEX': ('MEX1', 'Mexico', 1, 'Liga MX'),
    
    # Norway
    'NOR': ('NO1', 'Norway', 1, 'Eliteserien'),
    
    # Poland
    'POL': ('PL1', 'Poland', 1, 'Ekstraklasa'),
    
    # Romania
    'ROU': ('RO1', 'Romania', 1, 'Liga I'),
    
    # Russia
    'RUS': ('RU1', 'Russia', 1, 'Premier League'),
    
    # Sweden
    'SWE': ('SE1', 'Sweden', 1, 'Allsvenskan'),
    
    # Switzerland
    'SWZ': ('C1', 'Switzerland', 1, 'Super League'),
    
    # USA
    'USA': ('MLS1', 'United States', 1, 'MLS'),
}


def get_league_info(football_data_code: str) -> dict:
    """
    Get league information from a football-data.co.uk league code.
    
    Args:
        football_data_code: The league code from football-data.co.uk (e.g., 'E0', 'D1')
    
    Returns:
        Dictionary with keys: tm_code, country, tier, full_name
        Returns None values if code not found
    """
    if football_data_code in FOOTBALL_DATA_TO_TM_LEAGUE_MAP:
        tm_code, country, tier, full_name = FOOTBALL_DATA_TO_TM_LEAGUE_MAP[football_data_code]
        return {
            'tm_code': tm_code,
            'country': country,
            'tier': tier,
            'full_name': full_name
        }
    return {
        'tm_code': None,
        'country': None,
        'tier': None,
        'full_name': None
    }


def get_all_leagues_by_country(country: str) -> list:
    """
    Get all football-data codes for leagues in a specific country.
    
    Args:
        country: The country name (e.g., 'England', 'Spain')
    
    Returns:
        List of tuples: (football_data_code, tm_code, tier, full_name)
    """
    return [
        (fd_code, tm_code, tier, full_name)
        for fd_code, (tm_code, ctry, tier, full_name) in FOOTBALL_DATA_TO_TM_LEAGUE_MAP.items()
        if ctry == country
    ]


def get_tm_code_from_fd_code(football_data_code: str) -> str:
    """
    Get Transfermarkt code from football-data.co.uk code.
    
    Args:
        football_data_code: The league code from football-data.co.uk
    
    Returns:
        Transfermarkt league code or None if not found
    """
    info = get_league_info(football_data_code)
    return info['tm_code']
