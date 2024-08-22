set of int: cset = {4, 2, 8, 16}; % Forbidden values
set of int: d = 0..20; % Finite domain for vset
set of int: allowed_domain = d diff cset; % Domain excluding forbidden values

% Variable set vset defined as a subset of allowed_domain
var set of allowed_domain: vset;

constraint
    forall(i in cset) (
        not (i in vset)
    );
constraint
    forall(i in vset) (
      i mod 2 == 0
    );

solve satisfy;

output ["vset: \(vset)"];