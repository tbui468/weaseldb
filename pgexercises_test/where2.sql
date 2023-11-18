select facid, name, membercost, monthlymaintenance
    from facilities
    where
        membercost > 0 and
        (membercost < monthlymaintenance/50.0);
