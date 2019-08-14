#ifndef FWD_LIST_H
#define FWD_LIST_H

struct fwd_list_head {
	struct fwd_list_head *prev, *next;
};

#define FWD_LIST_HEAD_INIT(name) { &(name), &(name) }

#define FWD_LIST_HEAD(name) \
	struct fwd_list_head name = FWD_LIST_HEAD_INIT(name)

#define fwd_offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)

#define fwd_container_of(ptr, type, member) ({				\
	const typeof( ((type *)0)->member ) *__mptr = (ptr);	\
	(type *)( (char *)__mptr - fwd_offsetof(type, member) );})

#define fwd_list_entry(ptr, type, member) \
	fwd_container_of(ptr, type, member)

#define fwd_list_for_each_entry(pos, head, member)						\
	for (pos = fwd_list_entry((head)->next, typeof(*pos), member);		\
			&pos->member != (head);										\
			pos = fwd_list_entry(pos->member.next, typeof(*pos), member))

#define fwd_list_for_each_entry_safe(pos, n, head, member)					\
	for (pos = fwd_list_entry((head)->next, typeof(*pos), member),			\
			n = fwd_list_entry(pos->member.next, typeof(*pos), member);		\
			&pos->member != (head);											\
			pos = n, n = fwd_list_entry(n->member.next, typeof(*n), member))

void init_fwd_list_head(struct fwd_list_head *list);

void __fwd_list_add(struct fwd_list_head *new, struct fwd_list_head *prev, struct fwd_list_head *next);
void __fwd_list_del(struct fwd_list_head * prev, struct fwd_list_head * next);

void fwd_list_add(struct fwd_list_head *new, struct fwd_list_head *head);
void fwd_list_add_tail(struct fwd_list_head *new, struct fwd_list_head *head);

void fwd_list_del(struct fwd_list_head *entry);
void fwd_list_del_init(struct fwd_list_head *entry);

int fwd_list_empty(const struct fwd_list_head *head);

#endif
