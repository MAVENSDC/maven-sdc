pro idl_orbit_ql

;Determine the available orbits
get_orbit_times, '/maven/data/anc/orb/maven_orb_rec.orb', orbit_nums, dates, hhmmss
num_orbits = size(orbit_nums, /n_elements)

;Determine the latest orbit plotted
orb_plots = file_search("/maven/data/sdc/orbit_quicklooks/*")
sorted_orb_plots = orb_plots[sort(orb_plots)]
latest_orb_plot = sorted_orb_plots[-1]
latest_orbit = FIX(STRMID(latest_orb_plot,9,6, /REVERSE))

;Plot all orbits (except for the very last, we will not have enough data)
for i=0L, num_orbits-2 do begin
  if orbit_nums[i] le latest_orbit then continue
  fname = '/maven/data/sdc/orbit_quicklooks/mvn_orbit_ql_'+dates[i]+'_'+string(orbit_nums[i], format='(I6.6)')
  mvn_orbit_survey_plot_ephemeris, dates[i]+'/'+hhmmss[i], fname
  file_chmod, fname+".jpg", '644'o
endfor

end