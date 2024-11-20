function CREATE_TIME_STRING, year, mo_abr, day
  ;; Convert month 3 letter string rep into #
  case mo_abr of
    'JAN': mo_num = '01'
    'FEB': mo_num = '02'
    'MAR': mo_num = '03'
    'APR': mo_num = '04'
    'MAY': mo_num = '05'
    'JUN': mo_num = '06'
    'JUL': mo_num = '07'
    'AUG': mo_num = '08'
    'SEP': mo_num = '09'
    'OCT': mo_num = '10'
    'NOV': mo_num = '11'
    'DEC': mo_num = '12'
    ELSE: message, "Unrecognized three letter month abbreviation "
  endcase
  
  return, year+'-'+mo_num+'-'+day
end

pro get_orbit_times, file, orbnums, dates, hhmmss

  orbit_file = file
  CD, current=c
  orbit_file_template = c+'/orbit_template.sav'
  
  restore,orbit_file_template
  orbits = read_ascii(orbit_file ,template=orbit_template)
  orbnums = orbits.orbitnum
  periapsis_dates = []
  periapsis_hhmmss = []
  length = size(orbnums, /n_elements)
  for index = 0L, length-1 do begin
    bt_year = string(orbits.year(index))
    bt_mo_string = string(orbits.month(index))
    bt_day = string(orbits.day(index))
    bt_hhmmss = string(orbits.hhmmss(index))
    periapsis_dates = [periapsis_dates, create_time_string(bt_year, bt_mo_string, bt_day)]
    periapsis_hhmmss = [periapsis_hhmmss, bt_hhmmss]
  endfor

  dates = periapsis_dates
  hhmmss = periapsis_hhmmss
end 


