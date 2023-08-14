#define _FILE_OFFSET_BITS 64

#include <sys/types.h>
#include <sys/socket.h>
#ifdef __linux__
#if __GLIBC__ > 2 || __GLIBC_MINOR__ > 24
#include <sys/random.h>
#else
#include <sys/syscall.h>
#endif
#endif
#include <sys/uio.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/alloc.h>
#include <caml/unixsupport.h>
#include <caml/bigarray.h>
#include <caml/socketaddr.h>

#include "fork_action.h"

#ifdef ARCH_SIXTYFOUR
#define Int63_val(v) Long_val(v)
#define caml_copy_int63(v) Val_long(v)
#else
#define Int63_val(v) (Int64_val(v)) >> 1
#define caml_copy_int63(v) caml_copy_int64(v << 1)
#endif

static void caml_stat_free_preserving_errno(void *ptr) {
  int saved = errno;
  caml_stat_free(ptr);
  errno = saved;
}

CAMLprim value caml_eio_posix_getrandom(value v_ba, value v_off, value v_len) {
  CAMLparam1(v_ba);
  ssize_t ret;
  ssize_t off = (ssize_t)Long_val(v_off);
  ssize_t len = (ssize_t)Long_val(v_len);
  do {
    void *buf = (uint8_t *)Caml_ba_data_val(v_ba) + off;
    caml_enter_blocking_section();
#ifdef __linux__
#if __GLIBC__ > 2 || __GLIBC_MINOR__ > 24
    ret = getrandom(buf, len, 0);
#else
    ret = syscall(SYS_getrandom, buf, len, 0);
#endif
#else
    arc4random_buf(buf, len);
    ret = len;
#endif
    caml_leave_blocking_section();
  } while (ret == -1 && errno == EINTR);
  if (ret == -1) uerror("getrandom", Nothing);
  CAMLreturn(Val_long(ret));
}

/* Fill [iov] with pointers to the cstructs in the array [v_bufs]. */
static void fill_iov(struct iovec *iov, value v_bufs) {
  int n_bufs = Wosize_val(v_bufs);
  for (int i = 0; i < n_bufs; i++) {
    value v_cs = Field(v_bufs, i);
    value v_ba = Field(v_cs, 0);
    value v_off = Field(v_cs, 1);
    value v_len = Field(v_cs, 2);
    iov[i].iov_base = (uint8_t *)Caml_ba_data_val(v_ba) + Long_val(v_off);
    iov[i].iov_len = Long_val(v_len);
  }
}

CAMLprim value caml_eio_posix_readv(value v_fd, value v_bufs) {
  CAMLparam1(v_bufs);
  ssize_t r;
  int n_bufs = Wosize_val(v_bufs);
  struct iovec iov[n_bufs];

  fill_iov(iov, v_bufs);

  r = readv(Int_val(v_fd), iov, n_bufs);
  if (r < 0) uerror("readv", Nothing);

  CAMLreturn(Val_long(r));
}

CAMLprim value caml_eio_posix_writev(value v_fd, value v_bufs) {
  CAMLparam1(v_bufs);
  ssize_t r;
  int n_bufs = Wosize_val(v_bufs);
  struct iovec iov[n_bufs];

  fill_iov(iov, v_bufs);

  r = writev(Int_val(v_fd), iov, n_bufs);
  if (r < 0) uerror("writev", Nothing);

  CAMLreturn(Val_long(r));
}

CAMLprim value caml_eio_posix_preadv(value v_fd, value v_bufs, value v_offset) {
  CAMLparam2(v_bufs, v_offset);
  ssize_t r;
  int n_bufs = Wosize_val(v_bufs);
  struct iovec iov[n_bufs];

  fill_iov(iov, v_bufs);

  r = preadv(Int_val(v_fd), iov, n_bufs, Int63_val(v_offset));
  if (r < 0) uerror("preadv", Nothing);

  CAMLreturn(Val_long(r));
}

CAMLprim value caml_eio_posix_pwritev(value v_fd, value v_bufs, value v_offset) {
  CAMLparam2(v_bufs, v_offset);
  ssize_t r;
  int n_bufs = Wosize_val(v_bufs);
  struct iovec iov[n_bufs];

  fill_iov(iov, v_bufs);

  r = pwritev(Int_val(v_fd), iov, n_bufs, Int63_val(v_offset));
  if (r < 0) uerror("pwritev", Nothing);

  CAMLreturn(Val_long(r));
}

CAMLprim value caml_eio_posix_openat(value v_dirfd, value v_pathname, value v_flags, value v_mode) {
  CAMLparam1(v_pathname);
  char* pathname;
  int r;

  caml_unix_check_path(v_pathname, "openat");
  pathname = caml_stat_strdup(String_val(v_pathname));

  caml_enter_blocking_section();
  r = openat(Int_val(v_dirfd), pathname, Int_val(v_flags), Int_val(v_mode));
  caml_leave_blocking_section();

  caml_stat_free_preserving_errno(pathname);
  if (r < 0) uerror("openat", v_pathname);
  CAMLreturn(Val_int(r));
}

CAMLprim value caml_eio_posix_mkdirat(value v_fd, value v_path, value v_perm) {
  CAMLparam1(v_path);
  char *path;
  int ret;
  caml_unix_check_path(v_path, "mkdirat");
  path = caml_stat_strdup(String_val(v_path));
  caml_enter_blocking_section();
  ret = mkdirat(Int_val(v_fd), path, Int_val(v_perm));
  caml_leave_blocking_section();
  caml_stat_free_preserving_errno(path);
  if (ret == -1) uerror("mkdirat", v_path);
  CAMLreturn(Val_unit);
}

static double double_of_timespec(struct timespec *t) {
  return ((double) t->tv_sec) + (((double ) t->tv_nsec) / 1e9);
}

CAMLprim value caml_eio_posix_fstatat(value v_fd, value v_path, value v_flags) {
  CAMLparam1(v_path);
  CAMLlocal1(v_ret);
  char *path;
  int ret;
  struct stat statbuf;

  caml_unix_check_path(v_path, "fstatat");
  path = caml_stat_strdup(String_val(v_path));
  caml_enter_blocking_section();
  ret = fstatat(Int_val(v_fd), path, &statbuf, Int_val(v_flags));
  caml_leave_blocking_section();
  caml_stat_free_preserving_errno(path);
  if (ret == -1) uerror("fstatat", v_path);

  v_ret = caml_alloc_small(12, 0);
  Store_field(v_ret, 0, caml_copy_int64(statbuf.st_dev));
  Store_field(v_ret, 1, caml_copy_int64(statbuf.st_ino));
  Store_field(v_ret, 2, caml_hash_variant("Unknown"));	// TODO
  Store_field(v_ret, 3, Val_int(statbuf.st_mode & ~S_IFMT));
  Store_field(v_ret, 4, caml_copy_int64(statbuf.st_nlink));
  Store_field(v_ret, 5, caml_copy_int64(statbuf.st_uid));
  Store_field(v_ret, 6, caml_copy_int64(statbuf.st_gid));
  Store_field(v_ret, 7, caml_copy_int64(statbuf.st_rdev));
  Store_field(v_ret, 8, caml_copy_int63(statbuf.st_size));
  Store_field(v_ret, 9, caml_copy_double(double_of_timespec(&statbuf.st_atim)));
  Store_field(v_ret, 10, caml_copy_double(double_of_timespec(&statbuf.st_mtim)));
  Store_field(v_ret, 11, caml_copy_double(double_of_timespec(&statbuf.st_ctim)));

  CAMLreturn(v_ret);
}

CAMLprim value caml_eio_posix_unlinkat(value v_fd, value v_path, value v_dir) {
  CAMLparam1(v_path);
  char *path;
  int flags = Bool_val(v_dir) ? AT_REMOVEDIR : 0;
  int ret;
  caml_unix_check_path(v_path, "unlinkat");
  path = caml_stat_strdup(String_val(v_path));
  caml_enter_blocking_section();
  ret = unlinkat(Int_val(v_fd), path, flags);
  caml_leave_blocking_section();
  caml_stat_free_preserving_errno(path);
  if (ret == -1) uerror("unlinkat", v_path);
  CAMLreturn(Val_unit);
}

CAMLprim value caml_eio_posix_renameat(value v_old_fd, value v_old_path, value v_new_fd, value v_new_path) {
  CAMLparam2(v_old_path, v_new_path);
  size_t old_path_len = caml_string_length(v_old_path);
  size_t new_path_len = caml_string_length(v_new_path);
  char *old_path;
  char *new_path;
  int ret;
  caml_unix_check_path(v_old_path, "renameat-old");
  caml_unix_check_path(v_new_path, "renameat-new");
  old_path = caml_stat_alloc(old_path_len + new_path_len + 2);
  new_path = old_path + old_path_len + 1;
  memcpy(old_path, String_val(v_old_path), old_path_len + 1);
  memcpy(new_path, String_val(v_new_path), new_path_len + 1);
  caml_enter_blocking_section();
  ret = renameat(Int_val(v_old_fd), old_path,
                 Int_val(v_new_fd), new_path);
  caml_leave_blocking_section();
  caml_stat_free_preserving_errno(old_path);
  if (ret == -1) uerror("renameat", v_old_path);
  CAMLreturn(Val_unit);
}

CAMLprim value caml_eio_posix_spawn(value v_errors, value v_actions) {
  CAMLparam1(v_actions);
  pid_t child_pid;

  child_pid = fork();
  if (child_pid == 0) {
    eio_unix_run_fork_actions(Int_val(v_errors), v_actions);
  } else if (child_pid < 0) {
    uerror("fork", Nothing);
  }

  CAMLreturn(Val_long(child_pid));
}

/* Copy [n_fds] from [v_fds] to [msg]. */
static void fill_fds(struct msghdr *msg, int n_fds, value v_fds) {
  if (n_fds > 0) {
    int i;
    struct cmsghdr *cm;
    cm = CMSG_FIRSTHDR(msg);
    cm->cmsg_level = SOL_SOCKET;
    cm->cmsg_type = SCM_RIGHTS;
    cm->cmsg_len = CMSG_LEN(n_fds * sizeof(int));
    for (i = 0; i < n_fds; i++) {
      int fd = -1;
      if (Is_block(v_fds)) {
	fd = Int_val(Field(v_fds, 0));
	v_fds = Field(v_fds, 1);
      }
      ((int *)CMSG_DATA(cm))[i] = fd;
    }
  }
}

CAMLprim value caml_eio_posix_send_msg(value v_fd, value v_n_fds, value v_fds, value v_dst_opt, value v_bufs) {
  CAMLparam3(v_fds, v_dst_opt, v_bufs);
  int n_bufs = Wosize_val(v_bufs);
  int n_fds = Int_val(v_n_fds);
  struct iovec iov[n_bufs];
  union sock_addr_union dst_addr;
  int controllen = n_fds > 0 ? CMSG_SPACE(sizeof(int) * n_fds) : 0;
  char cmsg[controllen];
  struct msghdr msg = {
    .msg_iov = iov,
    .msg_iovlen = n_bufs,
    .msg_control = n_fds > 0 ? cmsg : NULL,
    .msg_controllen = controllen,
  };
  ssize_t r;

  memset(cmsg, 0, controllen);

  if (Is_some(v_dst_opt)) {
    caml_unix_get_sockaddr(Some_val(v_dst_opt), &dst_addr, &msg.msg_namelen);
    msg.msg_name = &dst_addr;
  }

  fill_iov(iov, v_bufs);
  fill_fds(&msg, n_fds, v_fds);

  caml_enter_blocking_section();
  r = sendmsg(Int_val(v_fd), &msg, 0);
  caml_leave_blocking_section();
  if (r < 0) uerror("send_msg", Nothing);

  CAMLreturn(Val_long(r));
}

static value get_msghdr_fds(struct msghdr *msg) {
  CAMLparam0();
  CAMLlocal2(v_list, v_cons);
  struct cmsghdr *cm;
  v_list = Val_int(0);
  for (cm = CMSG_FIRSTHDR(msg); cm; cm = CMSG_NXTHDR(msg, cm)) {
    if (cm->cmsg_level == SOL_SOCKET && cm->cmsg_type == SCM_RIGHTS) {
      int *fds = (int *) CMSG_DATA(cm);
      int n_fds = (cm->cmsg_len - CMSG_LEN(0)) / sizeof(int);
      int i;
      for (i = n_fds - 1; i >= 0; i--) {
	value fd = Val_int(fds[i]);
	v_cons = caml_alloc_tuple(2);
	Store_field(v_cons, 0, fd);
	Store_field(v_cons, 1, v_list);
	v_list = v_cons;
      }
    }
  }
  CAMLreturn(v_list);
}

CAMLprim value caml_eio_posix_recv_msg(value v_fd, value v_max_fds, value v_bufs) {
  CAMLparam1(v_bufs);
  CAMLlocal2(v_result, v_addr);
  int max_fds = Int_val(v_max_fds);
  int n_bufs = Wosize_val(v_bufs);
  struct iovec iov[n_bufs];
  union sock_addr_union source_addr;
  int controllen = max_fds > 0 ? CMSG_SPACE(sizeof(int) * max_fds) : 0;
  char cmsg[controllen];
  struct msghdr msg = {
    .msg_name = &source_addr,
    .msg_namelen = sizeof(source_addr),
    .msg_iov = iov,
    .msg_iovlen = n_bufs,
    .msg_control = max_fds > 0 ? cmsg : NULL,
    .msg_controllen = controllen,
  };
  ssize_t r;

  memset(cmsg, 0, controllen);

  fill_iov(iov, v_bufs);

  caml_enter_blocking_section();
  r = recvmsg(Int_val(v_fd), &msg, 0);
  caml_leave_blocking_section();
  if (r < 0) uerror("recv_msg", Nothing);

  v_addr = caml_unix_alloc_sockaddr(&source_addr, msg.msg_namelen, -1);

  v_result = caml_alloc_tuple(3);
  Store_field(v_result, 0, v_addr);
  Store_field(v_result, 1, Val_long(r));
  Store_field(v_result, 2, get_msghdr_fds(&msg));

  CAMLreturn(v_result);
}
