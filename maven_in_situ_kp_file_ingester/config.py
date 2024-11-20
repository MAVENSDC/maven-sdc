
kp_ingest_status_complete = 'COMPLETE'
kp_ingest_status_started = 'STARTED'
kp_ingest_status_deprecated = 'DEPRECATED'
kp_ingest_status_updated = 'UPDATED'
kp_ingest_status_failed = 'FAILED'

a16_format = 'A16'

process_quality_data = True

max_query_parameter_name_length = 63

# Size of KP data inserts
batch_size = 1000

# Process Pool Size
pool_size = 1

# Number of threads working per process on inserts
# if less than 2 no threads are created.
num_threads = 1

a16_default_conversions = {'nan': float('nan')}

'''
Dictionary of parameters for A16 values and their associated floating point values

Formatting of dictionary:

        { Parameter : {'non-numerical value1': floating point data value1,
                       'non-numerical value2': floating point data value2, ...}}
'''

ngims_sc_potential_flags = {'TSC:SC0': 0,
                            'TSC:SCP': 1,
                            'TSC:SCH': 2,
                            'TSU:SC0': 3,
                            'TSU:SCP': 4,
                            'TSU:SCH': 5}

ngims_neutral_io_flags = {'TSC:NIV:U': 1,
                          'TSC:NIU:U': 2,
                          'TSC:NOV:U': 3,
                          'TSC:NOU:U': 4,
                          'TSC:NIV:R': 5,
                          'TSC:NIU:R': 6,
                          'TSC:NOV:R': 7,
                          'TSC:NOU:R': 8,
                          'TSU:NIV:U': 9,
                          'TSU:NIU:U': 10,
                          'TSU:NOV:U': 11,
                          'TSU:NOU:U': 12,
                          'TSU:NIV:R': 13,
                          'TSU:NIU:R': 14,
                          'TSU:NOV:R': 15,
                          'TSU:NOU:R': 16}

a16_conversions = {
    'inbound_outbound_flag': {'I': 0,
                              'O': 1,
                              'I/O': 2},

    'ngims_ion_density_amu_17plus_quality': ngims_sc_potential_flags,

    'ngims_ar_density_quality': ngims_neutral_io_flags,

    'ngims_ion_density_amu_12plus_quality': ngims_sc_potential_flags,

    'spice_inbound_outbound_flag': {'I': 0,
                                    'O': 1},

    'ngims_ion_density_amu_14plus_quality': ngims_sc_potential_flags,

    'ngims_ion_density_amu_28plus_quality': ngims_sc_potential_flags,

    'ngims_co2_density_quality': ngims_neutral_io_flags,

    'ngims_ion_density_amu_30plus_quality': ngims_sc_potential_flags,

    'ngims_he_density_quality': ngims_neutral_io_flags,

    'ngims_no_density_quality': ngims_neutral_io_flags,

    'ngims_co_density_quality': ngims_neutral_io_flags,

    'ngims_n2_density_quality': ngims_neutral_io_flags,

    'ngims_ion_density_amu_44plus_quality': ngims_sc_potential_flags,

    'ngims_ion_density_amu_16plus_quality': ngims_sc_potential_flags,

    'ngims_ion_density_amu_32plus_quality': ngims_sc_potential_flags,

    'ngims_o_density_quality': ngims_neutral_io_flags
}
