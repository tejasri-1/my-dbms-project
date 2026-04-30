# Copyright (c) 2022-2026, PostgreSQL Global Development Group

auto_index_advisor_sources = files(
  'auto_index_advisor.c',
)

if host_system == 'windows'
  auto_index_advisor_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'auto_index_advisor',
    '--FILEDESC', 'auto_index_advisor - background worker for online index-advisor observation',])
endif

auto_index_advisor = shared_module('auto_index_advisor',
  auto_index_advisor_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += auto_index_advisor

install_data(
  'auto_index_advisor--1.0.sql',
  'auto_index_advisor.control',
  kwargs: contrib_data_args,
)
