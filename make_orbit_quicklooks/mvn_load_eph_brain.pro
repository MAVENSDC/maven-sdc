;+
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; mvn_load_eph_brain.pro
;
; Procedure to create tplot variables containing ephemeris information
; at a given time resolution over a given range. I realize
; there's a routine for this in the Berkeley distribution. I
; wanted things organized my own way, and had already started creating
; something when I found it.
;
; Syntax:
;      mvn_load_eph_brain, trange, 30d0
;
; Inputs:
;      trange            - timerange over which to load ephemeris
;
;      res               - time resolution, in seconds. Default = 60
;
; Dependencies:
;      none outside of Berkeley MAVEN software (I think)
;
; Dave Brain
; 19 February, 2015 - Initial version (approximate date). Not the
;                     prettiest code ever.
; 30 June, 2017 - Update to include plasma regions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;-
pro mvn_load_eph_brain, trange, res
  
;;; Figure out time resolution
  if n_params() lt 2 then res = 60d0
  
;;; Make time array based on trange
   dur = time_double(trange[1]) - time_double(trange[0])
   num = floor(dur/double(res)) + 1
   time = dindgen(num)*res + time_double(trange[0])

;;; Retrieve MVN position in Geographic coordinates
   geo_cart = spice_body_pos( 'MAVEN', 'MARS', frame='IAU_MARS', utc=time )

;;; Find lon/lat/rad
   geo_sph = cv_coord( from_rect=geo_cart, /to_sphere, /double )
   lon = ( reform( geo_sph[0,*] ) * !radeg + 360. ) mod 360.
   lat = reform( geo_sph[1,*] ) * !radeg
   rad = reform( geo_sph[2,*] )

;;; Find altitude
   mav_get_altitude, lon, lat, rad, alt, /areoid

;;; Store geographic info in tplot
   store_data, 'mvn_eph_geo', data={ x:time, y:geo_cart }
   store_data, 'mvn_lon', data={ x:time, y:lon }
   ylim, 'mvn_lon', 0, 360, 0
   options, 'mvn_lon', 'yticks', 4
   options, 'mvn_lon', 'ytitle', 'E Lon'
   store_data, 'mvn_lat', data={ x:time, y:lat }
   ylim, 'mvn_lat', -90, 90, 0
   options, 'mvn_lat', 'yticks', 4
   options, 'mvn_lat', 'ytitle', 'Lat'
   store_data, 'mvn_rad', data={ x:time, y:rad }
   options, 'mvn_rad', 'ytitle', 'Rad!C!C(km)'
   store_data, 'mvn_alt', data={ x:time, y:alt }
   options, 'mvn_alt', 'ylog', 1
   options, 'mvn_alt', 'ytitle', 'Alt!C!C(km)'

;;; Retrieve MVN position in MSO coordinates
   mso_cart = spice_body_pos( 'MAVEN', 'MARS', frame='MAVEN_MSO', utc=time )

;;; Find SZA, LST, SOLLAT
   mso_sph = cv_coord( from_rect=mso_cart, /to_sphere, /double )
   lst = ( ( reform( mso_sph[0,*] ) * !radeg + 540. ) mod 360. ) * 24./360.
   sollat = reform( mso_sph[1,*] ) * !radeg
   sza = reform( atan( sqrt( total( mso_cart[1:2,*]^2, 1) ), $
                       mso_cart[0,*] ) ) * !radeg

;;; Store MSO in tplot
   store_data, 'mvn_eph_mso', data={ x:time, y:mso_cart }
   store_data, 'mvn_lst', data={ x:time, y:lst }
   ylim, 'mvn_lst', 0, 24, 0
   options, 'mvn_lst', 'yticks', 4
   options, 'mvn_lst', 'ytitle', 'LT'
   store_data, 'mvn_sollat', data={ x:time, y:sollat }
   ylim, 'mvn_sollat', -90, 90, 0
   options, 'mvn_sollat', 'yticks', 4
   options, 'mvn_sollat', 'ytitle', 'Solar Latitude'
   store_data, 'mvn_sza', data={ x:time, y:sza }
   ylim, 'mvn_sza', 0, 180, 0
   options, 'mvn_sza', 'yticks', 4
   options, 'mvn_sza', 'ytitle', 'SZA'

;;; Make Sun bar
   sun = fix(time*0)
   tmp = where( mso_cart[0,*] ge 0 or $
                sqrt( mso_cart[1,*]^2 + mso_cart[2,*]^2 ) ge 3397., tmpnum )
   if tmpnum gt 0 then sun[tmp] = 1
   bararr = [[sun],[sun]]*60 + 40
   store_data, $
      'mvn_sun', $
      data={ x:time, $
	     y:bararr, $
             v:[0,1] }		
   options, 'mvn_sun', 'panel_size', .15
   options, 'mvn_sun', 'spec', 1
   ylim, 'mvn_sun', 0, 1, 0
   zlim, 'mvn_sun', 0, 250, 0
   options, 'mvn_sun', 'ytitle', ' '
   options, 'mvn_sun', 'yticks', 1
   options, 'mvn_sun', 'yminor', 1
   options, 'mvn_sun', 'no_color_scale', 1
   options, 'mvn_sun', 'x_no_interp', 1
   options, 'mvn_sun', 'y_no_interp', 1
   options, 'mvn_sun', 'xstyle', 4
   options, 'mvn_sun', 'ystyle', 4

;;; Figure out regions and store as a bar
   region = fltarr( n_elements(time) ) * !values.f_nan
   
   mso_x = mso_cart[0,*] / 3397.
   mso_y = mso_cart[1,*] / 3397.
   mso_z = mso_cart[2,*] / 3397.
   rho = sqrt( mso_y*mso_y + mso_z*mso_z )

   ;; Trotignon shock
   shock_epsilon = 1.026
   shock_L = 2.081
   shock_x0 = 0.6
   
   ;; Everything outside shock counts as solar wind
   xcomp = mso_x - shock_x0                       ; recenter on shock 'origin'
   alpha = atan( rho, xcomp ) < 160.*!dtor        ; angle from that origin
   actual_rad = sqrt( xcomp*xcomp + rho*rho )     ; Where is MAVEN?
   shock_rad = shock_L / ( 1. + shock_epsilon * cos(alpha) ) ; where is shock?
   region[ where( actual_rad ge shock_rad, /null ) ] = 3 ; Compare distances

   ;; Trotignon MPB
   mpb1_epsilon = 0.77
   mpb1_L       = 1.08
   mpb1_x0      = 0.64
   mpb2_epsilon = 1.009
   mpb2_L       = 0.528
   mpb2_x0      = 1.6
      
   ;; Everthing between shock and MPB counts as sheath
   xcomp = mso_x - mpb1_x0    
   alpha = atan( rho, xcomp ) < 160.*!dtor   
   actual_rad = sqrt( xcomp*xcomp + rho*rho ) 
   mpb_rad = mpb1_L / ( 1. + mpb1_epsilon * cos(alpha) )
   ; Are there post-terminator measurements?
   tmp = where( mso_x lt 0, tmpcnt ) 
   if tmpcnt gt 0 then begin
      xcomp = mso_x[tmp] - mpb2_x0     
      alpha = atan( rho[tmp], xcomp ) < 160.*!dtor 
      actual_rad[tmp] = sqrt( xcomp*xcomp + rho[tmp]*rho[tmp] )   
      mpb_rad[tmp] = mpb2_L / ( 1. + mpb2_epsilon * cos(alpha) )
   endif
   region[ where( actual_rad ge mpb_rad and $
                  finite(region) eq 0, /null ) ] = 2
   
   ;; Everything inside MPB counts as pileup
   region[ where( actual_rad lt mpb_rad and $
                  finite(region) eq 0, /null ) ] = 1
   
   ;; Everything in wake counts as wake
   region[ where( mso_x lt 0 and rho lt 1, /null ) ] = 0

   ;;; If alt lt 380 then overwrite as ionosphere
   ;region[ where( alt lt 380, /null ) ] = 4

   ;;; Show orbit in cylindrical coords, to check regions correct
   ;window, xs=1000,ys=600
   ;plot, [0], [0], /nodata, xrange=[-3,2], yrange=[0,3], /iso
   ;oplot, cos(findgen(180)*!dtor), sin(findgen(180)*!dtor)
   ;plot_shock, /trot
   ;plot_mpb, /trot
   ;oplot, mso_x, rho
   ;tmp = where(region eq 3, tmpcnt)                                  
   ;if tmpcnt gt 0 then oplot, mso_x[tmp], rho[tmp], color=2, thick=2, psym=8   
   ;tmp = where(region eq 2, tmpcnt)                               
   ;if tmpcnt gt 0 then oplot, mso_x[tmp], rho[tmp], color=0, thick=2, psym=8
   ;tmp = where(region eq 1, tmpcnt)                               
   ;if tmpcnt gt 0 then oplot, mso_x[tmp], rho[tmp], color=1, thick=2, psym=8  
   ;tmp = where(region eq 0, tmpcnt)                               
   ;if tmpcnt gt 0 then oplot, mso_x[tmp], rho[tmp], color=20, thick=2, psym=8

   ;;; Store as a bar
   bararr = [[region],[region]]*80
   store_data, $
      'mvn_region', $
      data={ x:time, $
	     y:bararr, $
             v:[0,1] }		
   options, 'mvn_region', 'panel_size', .15
   options, 'mvn_region', 'spec', 1
   ylim, 'mvn_region', 0, 1, 0
   zlim, 'mvn_region', 0, 250, 0
   options, 'mvn_region', 'ytitle', ' '
   options, 'mvn_region', 'yticks', 1
   options, 'mvn_region', 'yminor', 1
   options, 'mvn_region', 'no_color_scale', 1
   options, 'mvn_region', 'x_no_interp', 1
   options, 'mvn_region', 'y_no_interp', 1
   options, 'mvn_region', 'xstyle', 4
   options, 'mvn_region', 'ystyle', 4
   options, 'mvn_region', 'colors', [ 0, 80, 160, 240 ]
   options, 'mvn_region', 'labflag', 1
   options, 'mvn_region', 'labels', ['SW', 'Sheath', 'Pileup', 'Wake' ]
      
end
