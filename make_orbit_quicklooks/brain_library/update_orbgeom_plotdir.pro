; pro update_orbgeom_plotdir


;;; Parameters
   orbdir = '/Volumes/BrainMAVENData/maven_dave/orbgeom/'
   endtime = systime(1) - 86400d*7d

   
;;; Get the starting place
  ;; Get the name of the last file in the directory
  filelist = file_search( orbdir, '*.jpg' )
  lastfile = filelist[-1]
  ;; Get the orbit number associated with the time in the filename
  startpos = strpos(lastfile,'/',/reverse_search)
  lastfile = strmid(lastfile,startpos+1)
  lasttime = strmid(lastfile, 0,4) + '-' + $
             strmid(lastfile, 4,2) + '-' + $
             strmid(lastfile, 6,2) + '/' + $
             strmid(lastfile, 8,2) + ':' + $
             strmid(lastfile,10,2) + ':' + $
             strmid(lastfile,12,2)
  lastorbnum = mvn_orbit_num( time=time_double(lasttime) )
  curorbit = lastorbnum + 1
  curtime = mvn_orbit_num( orb = curorbit )

  
;;; Loop until done
  while curtime lt endtime do begin
     curtimestr = time_string(curtime)
     fileroot = strmid(curtimestr, 0,4) + $
                strmid(curtimestr, 5,2) + $
                strmid(curtimestr, 8,2) + $
                strmid(curtimestr,11,2) + $
                strmid(curtimestr,14,2) + $
                strmid(curtimestr,17,2)
     for i = 0, 4 do print, ' '
     print, orbdir+fileroot
     print, ' '
     mvn_orbit_survey_plot_ephemeris, curtime, orbdir+fileroot
     curorbit += 1
     curtime = mvn_orbit_num( orb = curorbit )
  endwhile
    
end
