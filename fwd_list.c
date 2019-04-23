#include "fwd_list.h"
#include <stdlib.h>

void init_fwd_list_head(struct fwd_list_head *list) {
	list->next = list;
	list->prev = list;
}

void __fwd_list_add(struct fwd_list_head *new, struct fwd_list_head *prev, struct fwd_list_head *next) {
	next->prev = new;
	new->next = next;
	new->prev = prev;
	prev->next = new;
}

void fwd_list_add(struct fwd_list_head *new, struct fwd_list_head *head) {
	__fwd_list_add(new, head, head->next);
}

void fwd_list_add_tail(struct fwd_list_head *new, struct fwd_list_head *head) {
	__fwd_list_add(new, head->prev, head);
}

void __fwd_list_del(struct fwd_list_head * prev, struct fwd_list_head * next) {
	next->prev = prev;
	prev->next = next;
}

void fwd_list_del(struct fwd_list_head *entry) {
	__fwd_list_del(entry->prev, entry->next);
	entry->next = entry;
	entry->prev = entry;
}

void __fwd_list_del_entry(struct fwd_list_head *entry) {
	__fwd_list_del(entry->prev, entry->next);
}

void fwd_list_del_init(struct fwd_list_head *entry) {
	__fwd_list_del_entry(entry);
	init_fwd_list_head(entry);
}

int fwd_list_empty(const struct fwd_list_head *head) {
	return head->next == head;
}
