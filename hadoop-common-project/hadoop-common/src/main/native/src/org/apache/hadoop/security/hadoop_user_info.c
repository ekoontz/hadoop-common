/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "hadoop_user_info.h"

#include <errno.h>
#include <grp.h>
#include <pthread.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define INITIAL_GIDS_SIZE 32

struct hadoop_user_info *hadoop_user_info_alloc(void)
{
  struct hadoop_user_info *uinfo;
  size_t buf_sz;
  char *buf;

  uinfo = calloc(1, sizeof(struct hadoop_user_info));
  buf_sz = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (buf_sz < 1024) {
    buf_sz = 1024;
  }
  buf = malloc(buf_sz);
  if (!buf) {
    free(uinfo);
    return NULL;
  }
  uinfo->buf_sz = buf_sz;
  uinfo->buf = buf;
  return uinfo;
}

static void hadoop_user_info_clear(struct hadoop_user_info *uinfo)
{
  struct passwd *pwd = &uinfo->pwd;

  pwd->pw_name = NULL;
  pwd->pw_uid = 0;
  pwd->pw_gid = 0;
  pwd->pw_passwd = NULL;
  pwd->pw_gecos = NULL;
  pwd->pw_dir = NULL;
  pwd->pw_shell = NULL;
  free(uinfo->gids);
  uinfo->gids = 0;
  uinfo->num_gids = 0;
  uinfo->gids_size = 0;
}

void hadoop_user_info_free(struct hadoop_user_info *uinfo)
{
  free(uinfo->buf);
  hadoop_user_info_clear(uinfo);
  free(uinfo);
}

/**
 * Different platforms use different error codes to represent "user not found."
 * So whitelist the errors which do _not_ mean "user not found."
 *
 * @param err           The errno
 *
 * @return              The error code to use
 */
static int getpwnam_error_translate(int err)
{
  if ((err == EIO) || (err == EMFILE) || (err == ENFILE) ||
      (err == ENOMEM) || (err == ERANGE)) {
    return err;
  }
  return ENOENT;
}

int hadoop_user_info_fetch(struct hadoop_user_info *uinfo,
                           const char *username)
{
  struct passwd *pwd;
  int err;
  size_t buf_sz;
  char *nbuf;

  hadoop_user_info_clear(uinfo);
  for (;;) {
    do {
      pwd = NULL;
      err = getpwnam_r(username, &uinfo->pwd, uinfo->buf,
                         uinfo->buf_sz, &pwd);
    } while ((!pwd) && (errno == EINTR));
    if (pwd) {
      return 0;
    }
    if (err != ERANGE) {
      return getpwnam_error_translate(errno);
    }
    buf_sz = uinfo->buf_sz * 2;
    nbuf = realloc(uinfo->buf, buf_sz);
    if (!nbuf) {
      return ENOMEM;
    }
    uinfo->buf = nbuf;
    uinfo->buf_sz = buf_sz;
  }
}

int hadoop_user_info_getgroups(struct hadoop_user_info *uinfo)
{
  int ret, ngroups;
  gid_t *ngids;

  if (!uinfo->pwd.pw_name) {
    return EINVAL; // invalid user info
  }
  uinfo->num_gids = 0;
  if (!uinfo->gids) {
    uinfo->gids = malloc(sizeof(uinfo->gids[0]) * INITIAL_GIDS_SIZE);
    if (!uinfo->gids) {
      return ENOMEM;
    }
    uinfo->gids_size = INITIAL_GIDS_SIZE;
  }
  ngroups = uinfo->gids_size;
  ret = getgrouplist(uinfo->pwd.pw_name, uinfo->pwd.pw_gid, 
                         uinfo->gids, &ngroups);
  if (ret != -1) {
    uinfo->num_gids = ngroups;
    return 0;
  }
  ngids = realloc(uinfo->gids, sizeof(uinfo->gids[0]) * ngroups);
  if (!ngids) {
    return ENOMEM;
  }
  uinfo->gids = ngids;
  uinfo->gids_size = ngroups;
  ret = getgrouplist(uinfo->pwd.pw_name, uinfo->pwd.pw_gid, 
                         uinfo->gids, &ngroups);
  if (ret != -1) {
    uinfo->num_gids = ngroups;
    return 0;
  }
  return EIO;
}

#ifdef USER_TESTING
/**
 * A main() is provided so that quick testing of this
 * library can be done. 
 */
int main(int argc, char **argv) {
  char **username, *prefix;
  struct hadoop_user_info *uinfo;
  int i, ret;
  
  uinfo = hadoop_user_info_alloc();
  if (!uinfo) {
    fprintf(stderr, "hadoop_user_info_alloc returned NULL.\n");
    return EXIT_FAILURE;
  }
  for (username = argv + 1; *username; username++) {
    ret = hadoop_user_info_fetch(uinfo, *username);
    if (!ret) {
      fprintf(stderr, "user[%s] : pw_uid = %lld\n",
              *username, (long long)uinfo->pwd.pw_uid);
    } else {
      fprintf(stderr, "user[%s] : error %d (%s)\n",
              *username, ret, strerror(ret));
    }
    ret = hadoop_user_info_getgroups(uinfo);
    if (!ret) {
      fprintf(stderr, "          getgroups: ");
      prefix = "";
      for (i = 0; i < uinfo->num_gids; i++) {
        fprintf(stderr, "%s%lld", prefix, (long long)uinfo->gids[i]);
        prefix = ", ";
      }
      fprintf(stderr, "\n");
    } else {
      fprintf(stderr, "          getgroups: error %d\n", ret);
    }
  }
  hadoop_user_info_free(uinfo);
  return EXIT_SUCCESS;
}
#endif
